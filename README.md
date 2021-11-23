# openlineage metrics

This repository contains an Airflow project designed for use with the Astro CLI, along with a series of dbt models that perform in-place transformations.

## Setup

1. Copy the service account JSON key into `openlineage.json`:

```bash
cat > openlineage.json
[paste]
^D
```

2. Copy the `.env-example` file to `.env` and edit it with your `OPENLINEAGE_URL` and `OPENLINEAGE_API_KEY` values.

```bash
cat > ~/.openlineage/auth
OPENLINEAGE_URL=https://localhost:5000
```

3. Start up the Astro dev environment:

```
astro dev start
```

