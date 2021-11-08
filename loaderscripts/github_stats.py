#!/usr/bin/env python3

import requests
import time
from google.cloud import bigquery
from google.oauth2 import service_account
import os

# Pull stats for these projects
projects = ['MarquezProject/marquez', 'OpenLineage/OpenLineage']

# Pull our key from this file
key_path = os.environ['HOME'] + "/.dbt/openlineage.json"

# Set up the BigQuery client
credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
client = bigquery.Client(credentials=credentials, project='openlineage')

# Select the right table
dataset_ref = client.dataset('metrics')
table_ref = dataset_ref.table('github_stars')
table = client.get_table(table_ref)

# Get the current time
now = int(time.time())

print('LOADER: fetching latest github star counts')
for project in projects:
  url = 'https://api.github.com/repos/' + project
  response = requests.get(url)
  watchers = response.json()['watchers']
  print('  project={:s}, watchers={:d}'.format(project,watchers))
  client.insert_rows(table, [(now,project,watchers)])

print('done\n\n')
