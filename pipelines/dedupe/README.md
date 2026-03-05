# Tenant Power Deduplication

## Setting Up a Local Environment

You'll need a `.env` file containing connection string parameters and keys for authentication (see [`.env.example`](https://github.com/mit-spatial-action/tenantpower/blob/main/pipelines/dedupe/.env.example)).

```sh
DB_USER=
DB_HOST=
DB_NAME=
DB_PASS=
DB_PORT=
```

You'll also need to source the relevant parcel datasets.

This project uses the `uv` package and project manager. [Consult the documentation for installation instructions](https://github.com/astral-sh/uv). Then, from the `backend` directory...

```bash
uv sync
uv run --env-file=.env main.py
```
