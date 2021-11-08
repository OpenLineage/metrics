# openlineage metrics

This repository contains a dbt project, along with a series of loaders that can be used to pull data from original sources.

## Setup

1. Copy the `dbt-profile.yml` example file to `~/.dbt/profile.yml`:

```bash
mkdir ~/.dbt
cp dbt-profile.yml ~/.dbt/profile.yml
```

2. Copy the service account JSON key into `~/.dbt/openlineage.json`:

```bash
cat > ~/.dbt/openlineage.json
[paste]
^D
```

3. Add the correct OpenLineage environment variables to `~/.openlineage/auth`:

```bash
cat > ~/.openlineage/auth
OPENLINEAGE_URL=https://localhost:5000
```

## Running from Docker

Build the image:

```bash
docker build -t openlineage-metrics .
```

Run the image:

```bash
docker run -v ~/.openlineage/auth:/root/.openlineage/auth:ro -v ~/.dbt/openlineage.json:/root/.dbt/openlineage.json:ro openlineage-metrics
```

## Running locally

Install dbt:

```bash
python3 -m venv virtualenv
source virtualenv/bin/activate
pip3 install dbt openlineage-dbt
```

Each of the loaders should work if you have the correct key in `~/.dbt/openlineage.yaml`.

```bash
# loaders/[loader].py
```

Set your OPENLINEAGE_KEY, OPENLINEAGE_NAMESPACE, and OPENLINEAGE_URL environment variables:

```bash
export OPENLINEAGE_URL=https://localhost:5000
```

Run dbt using this command:

```
dbt-ol run
```
