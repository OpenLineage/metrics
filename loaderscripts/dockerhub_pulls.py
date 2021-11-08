#!/usr/bin/env python3

import requests
import time
from google.cloud import bigquery
from google.oauth2 import service_account
import os

# Pull stats for these images
images = ['marquezproject/marquez', 'marquezproject/marquez-web']

# Pull our key from this file
key_path = os.environ['HOME'] + "/.dbt/openlineage.json"

# Set up the BigQuery client
credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
client = bigquery.Client(credentials=credentials, project='openlineage')

# Select the right table
dataset_ref = client.dataset('metrics')
table_ref = dataset_ref.table('dockerhub_pulls')
table = client.get_table(table_ref)

# Get the current time
now = int(time.time())

print('LOADER: fetching latest dockerhub pull counts')
for image in images:
  url = 'https://hub.docker.com/v2/repositories/' + image
  response = requests.get(url)
  pull_count = response.json()['pull_count']
  client.insert_rows(table, [(now,image,pull_count)])
  print('  image={:s}, pull_count={:d}'.format(image,pull_count))

print('done\n\n')
