# openlineage metrics

This repository contains an Airflow project designed for use with the Astro CLI, along with a series of dbt models that perform in-place transformations.

## Setup

1. Copy the `.env.example` file to `.env`:

```bash
cp .env.example .env
```

2. Edit the .env file with your `OPENLINEAGE_URL` and `OPENLINEAGE_API_KEY` values. Paste the BigQuery JSON key on a single line where indicated.

3. Start up the Astro dev environment:

```
astro dev start
```