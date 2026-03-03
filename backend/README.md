# Tenant Power Backend

## Setting Up a Local Environment

⚠️ ___Caveat emptor!___ ⚠️ This project depends on a PostgreSQL/PostGIS database. This is a quick-and-dirty FastAPI application that does not do anything to facilitate e.g., migrations, or ingestion. 

Assuming you have the database spun up, you'll need a `.env` file containing connection string parameters and keys for authentication (see [`.env.example`](https://github.com/mit-spatial-action/tenantpower.org/blob/main/backend/.env.example)).

```sh
DB_USER=
DB_HOST=
DB_NAME=
DB_PASS=
DB_PORT=
# Comma-separated list of API keys for endpoint authentication.
API_KEYS=","
```

This project uses the `uv` package and project manager. [Consult the documentation for installation instructions](https://github.com/astral-sh/uv). Then, from the `backend` directory...

```bash
uv sync
uv run --env-file=.env fastapi dev
```
