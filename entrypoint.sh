#!/bin/bash
#
# Usage: $ ./entrypoint.sh

set -e

# This is very insecure
cd /metrics
for x in loaders/*.py; do
    python3 $x
done

set -o allexport
source ~/.openlineage/auth
set +o allexport

echo "generating dbt catalog..."
dbt-ol docs generate

echo "running dbt seed"
dbt-ol seed

echo "running dbt"
dbt-ol run
