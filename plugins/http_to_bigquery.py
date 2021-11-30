
from typing import Any, Callable, Dict, Optional, Type
from requests.auth import AuthBase, HTTPBasicAuth
from datetime import datetime

from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.utils.operator_helpers import make_kwargs_callable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

class HttpToBigQueryOperator(BaseOperator):
    """
    Calls an HTTP endpoint and loads the response into a BigQuery table

    :param http_conn_id: The :ref:`http connection<howto/connection:http>` to run
        the operator against
    :type http_conn_id: str
    :param endpoint: The relative part of the full url. (templated)
    :type endpoint: str
    :param method: The HTTP method to use, default = "POST"
    :type method: str
    :param data: The data to pass. POST-data in POST/PUT and params
        in the URL for a GET request. (templated)
    :type data: For POST/PUT, depends on the content-type parameter,
        for GET a dictionary of key/value string pairs
    :param headers: The HTTP headers to be added to the GET request
    :type headers: a dictionary of string key/value pairs
    :param response_filter: A function allowing you to manipulate the response
        text. e.g response_filter=lambda response: json.loads(response.text).
        The callable takes the response object as the first positional argument
        and optionally any number of keyword arguments available in the context dictionary.
    :type response_filter: A lambda or defined function.
    :param extra_options: Extra options for the 'requests' library, see the
        'requests' documentation (options to modify timeout, ssl, etc.)
    :type extra_options: A dictionary of options, where key is string and value
        depends on the option that's being modified.
    :param log_response: Log the response (default: False)
    :type log_response: bool
    :param auth_type: The auth type for the service
    :type auth_type: AuthBase of python requests lib
    :param project_id: The Google cloud project in which to look for the
    table. The connection supplied to the hook must provide access to
    the specified project.
    :type project_id: str
    :param dataset_id: The name of the dataset in which to find or create the
        table.
    :type dataset_id: str
    :param table_id: The name of the table to store the HTTP response body in.
    :type table_id: str

    :param gcp_conn_id: The connection ID used to connect to Google Cloud and
        interact with the BigQuery service.
    :type gcp_conn_id: str
    :param location: [Optional] The geographic location of the job. Required except for US and EU.
        See details at https://cloud.google.com/bigquery/docs/locations#specifying_your_location
    :type location: str
    """

    template_fields = [
        'endpoint',
        'data',
        'headers',
        'project_id',
        'dataset_id',
        'table_id',
    ]

    template_fields_renderers = {'headers': 'json', 'data': 'py'}
    template_ext = ()
    ui_color = '#f4a460'

    def __init__(
        self,
        *,
        endpoint: Optional[str] = None,
        method: str = 'GET',
        data: Any = None,
        headers: Optional[Dict[str, str]] = None,
        response_filter: Optional[Callable[..., Any]] = None,
        extra_options: Optional[Dict[str, Any]] = None,
        http_conn_id: str = 'http_default',
        log_response: bool = False,
        auth_type: Type[AuthBase] = HTTPBasicAuth,
        project_id,
        dataset_id,
        table_id,
        gcp_conn_id='google_cloud_default',
        location=None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        self.http_conn_id = http_conn_id
        self.method = method
        self.endpoint = endpoint
        self.headers = headers or {}
        self.data = data or {}
        self.response_filter = response_filter
        self.extra_options = extra_options or {}
        self.log_response = log_response
        self.auth_type = auth_type
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.gcp_conn_id = gcp_conn_id
        self.location = location

        self.hook = None

    def execute(self, context: Dict[str, Any]) -> Any:

        self.log.info("Calling HTTP method")
        http = HttpHook(self.method, http_conn_id=self.http_conn_id, auth_type=self.auth_type)
        response = http.run(self.endpoint, self.data, self.headers, self.extra_options)

        if self.log_response:
            self.log.info(response.text)

        responseText = response.text

        if self.response_filter:
            kwargs_callable = make_kwargs_callable(self.response_filter)
            responseText = kwargs_callable(response.text, **context)

        if self.hook is None:
            self.log.info('Connecting to: %s', self.project_id)
            self.hook = BigQueryHook(
                gcp_conn_id=self.gcp_conn_id,
                use_legacy_sql=True,
                location=self.location,
            )

        self.log.info('Validating table: %s', self.table_id)

        self.hook.run_table_upsert(
            dataset_id=self.dataset_id,
            project_id=self.project_id,
            table_resource={
                "tableReference": {
                    "projectId": self.project_id,
                    "datasetId": self.dataset_id,
                    "tableId": self.table_id
                },
                "schema": {
                    "fields": [
                        {"name": "timestamp", "type": "TIMESTAMP"},
                        {"name": "http_response", "type": "STRING"},
                    ]
                },
            },
        )

        currentTime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        self.log.info('Insertng row into table %s for datestamp %s', self.table_id, currentTime)

        self.hook.insert_all(
            dataset_id=self.dataset_id,
            project_id=self.project_id,
            table_id=self.table_id,
            rows=[(currentTime, responseText)]
        )

    def on_kill(self) -> None:
        super().on_kill()
        if self.hook is not None:
            self.log.info('Cancelling running query')
            self.hook.cancel_query()
