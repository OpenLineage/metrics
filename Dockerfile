FROM fishtownanalytics/dbt:0.20.1

RUN pip install openlineage-dbt google-cloud-bigquery

WORKDIR /root
RUN mkdir -p .dbt
COPY dbt-profiles.yml .dbt/profiles.yml
RUN mkdir -p .openlineage
WORKDIR /metrics
COPY . .

ENTRYPOINT ["/metrics/entrypoint.sh"]