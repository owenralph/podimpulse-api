# PodImpulse API

Azure Functions API for podcast analytics workflows (ingest, trend, regression, prediction, impact, and missing-episode review).

## Prerequisites

- Python 3.11
- Azure Functions Core Tools (for local function runtime)
- Azure Storage (or Azurite for local integration testing)

## Setup

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Configure environment (for local execution):

- `BLOB_CONNECTION_STRING` (required)
- `FACEBOOK_APP_ID` and `FACEBOOK_APP_SECRET` (required for Facebook endpoints)

## Authentication Policy

All HTTP endpoints require an Azure Functions key (`AuthLevel.FUNCTION`).

- Query string: `?code=<function-key>`
- Header: `x-functions-key: <function-key>`

## API Paths (Current)

- `POST/GET /v1/podcasts`
- `GET/PUT/PATCH/DELETE /v1/podcasts/{podcast_id}`
- `POST/GET/DELETE /v1/podcasts/{podcast_id}/ingest`
- `GET/POST /v1/podcasts/{podcast_id}/missing`
- `POST/GET /v1/podcasts/{podcast_id}/predict`
- `POST/GET /v1/podcasts/{podcast_id}/regression`
- `GET /v1/podcasts/{podcast_id}/trend`
- `GET /v1/podcasts/{podcast_id}/impact`

Detailed schema is in `podimpulse.yaml`.

## Legacy Endpoints

Old top-level compute routes (for example `/v1/ingest`, `/v1/predict`, `/v1/trend`) now return `410 Gone` with the replacement resource path.
These responses also include a sunset date of `2026-06-30`.

Decommission plan:
1. Keep returning `410` + replacement path until all consumers migrate.
2. Validate no consumer traffic to legacy routes in Application Insights for 30 consecutive days.
3. Remove legacy route declarations from `function_app.py` after migration criteria are met.

## Tests

Unit tests:

```bash
python -m unittest tests.test_runtime_fixes -v
```

Azurite integration tests:

```bash
python -m unittest tests.test_blob_integration -v
```

If Azurite is not running locally, the integration suite skips.

## CI

GitHub Actions workflow: `.github/workflows/main_podimpulse.yml`

- `unit-tests` runs pure unit tests.
- `integration-azurite` runs blob integration tests against Azurite service container.
