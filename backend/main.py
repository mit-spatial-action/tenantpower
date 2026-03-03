import os
import json
from typing import Annotated, Set, Union
from fastapi import FastAPI, Depends, Security, status, APIRouter, Response, Path, Query
from fastapi.security import APIKeyHeader
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import computed_field, field_validator
import asyncpg


class Settings(BaseSettings):
    db_user: str
    db_host: str
    db_name: str
    db_pass: str
    db_port: str
    api_keys: Union[Set[str], str] = set()

    @field_validator("api_keys", mode="before")
    @classmethod
    def parse_api_keys(cls, v: str) -> Set[str]:
        if isinstance(v, str):
            return {key.strip() for key in v.split(",") if key.strip()}
        return v

    @computed_field
    @property
    def db_url(self) -> str:
        return f"postgresql://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db_name}"

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)


settings = Settings()

TITLE = "Tenant Power API"
VERSION = "1.0.0"
ROOT_PATH = "/tenantpower"

key_header = APIKeyHeader(name="X-API-Key", auto_error=True)


async def validate_key(key: str = Security(key_header)):
    if key not in settings.api_keys:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API Key",
        )
    return key


async def get_system_status(conn: asyncpg.Connection):
    try:
        await conn.fetchval("SELECT 1")
        return "running"
    except Exception as e:
        return str(e)


app = FastAPI(title=TITLE, version=VERSION, root_path=ROOT_PATH)

v1 = APIRouter(prefix="/v1", tags=["v1"], dependencies=[Depends(validate_key)])


@v1.get("/clusters/{id}")
async def get_clusters(
    id: Annotated[int, Path(title="The ID of the cluster to filter by")],
):
    conn = await asyncpg.connect(settings.db_url)
    try:
        query = """
            SELECT jsonb_build_object(
                'type', 'FeatureCollection',
                'features', jsonb_agg(features)
            )
            FROM (
                SELECT jsonb_build_object(
                    'type',       'Feature',
                    'id',         id,
                    'geometry',   ST_AsGeoJSON(geometry)::jsonb,
                    'properties', to_jsonb(row) - 'geometry'
                ) AS feature
                FROM (
                    SELECT * FROM props 
                    WHERE clst = $1 
                    ORDER BY clst ASC
                ) row
            ) features;
        """
        result = await conn.fetchval(query, id)
        if not result:
            return {"type": "FeatureCollection", "features": []}

        return Response(content=result, media_type="application/json")

    finally:
        await conn.close()


@v1.get("/props/{id}")
async def get_props(
    id: Annotated[int, Path(title="The ID of the property.")],
    bare: Annotated[bool, Query()],
):
    conn = await asyncpg.connect(settings.db_url)
    cols = "id, clst, geometry" if bare else "*"
    try:
        query = f"""
            SELECT jsonb_build_object(
                'type', 'FeatureCollection',
                'features', jsonb_agg(features)
            )
            FROM (
                SELECT jsonb_build_object(
                    'type',       'Feature',
                    'id',         id,
                    'geometry',   ST_AsGeoJSON(geometry)::jsonb,
                    'properties', to_jsonb(row) - 'geometry'
                ) AS feature
                FROM (
                    SELECT {cols} FROM props 
                    WHERE id = $1 
                    ORDER BY clst ASC
                ) row
            ) features;
        """
        result = await conn.fetchval(query, id)
        if not result:
            return {"type": "FeatureCollection", "features": []}

        return Response(content=result, media_type="application/json")

    finally:
        await conn.close()


@v1.get("/props_by_loc/")
async def get_props_by_loc(
    lat: Annotated[float, Query(gt=-90, lt=90)],
    lng: Annotated[float, Query(gt=-180, lt=180)],
    n: Annotated[int, Query(gt=0)],
    bare: Annotated[bool, Query()],
):
    conn = await asyncpg.connect(settings.db_url)
    cols = "id, clst, geometry" if bare else "*"
    try:
        query = f"""
            SELECT jsonb_build_object(
                'type', 'FeatureCollection',
                'features', jsonb_agg(ST_AsGeoJSON(t.*)::jsonb)
            )
            FROM ( 
                SELECT {cols} FROM props
                WHERE ST_DWithin(
                    ST_Transform(geometry, 2249),
                    ST_Transform(ST_SetSRID(ST_Point($1, $2), 4326), 2249),
                    100
                )
                ORDER BY ST_Transform(geometry, 2249) <-> ST_Transform(ST_SetSRID(ST_Point($1, $2), 4326), 2249)
                LIMIT $3
            ) AS t;
        """
        result = await conn.fetchval(query, lng, lat, n)
        if not result:
            return {"type": "FeatureCollection", "features": []}

        return Response(content=result, media_type="application/json")

    finally:
        await conn.close()


app.include_router(v1)


@app.get("/")
async def get_root():
    conn = await asyncpg.connect(settings.db_url)
    status = await get_system_status(conn)
    return {
        "app": TITLE,
        "version": VERSION,
        "status": status,
        "links": {
            "swagger_ui": os.path.join(ROOT_PATH, "/docs"),
            "redoc": os.path.join(ROOT_PATH, "/redoc"),
            "openapi_spec": app.openapi_url,
        },
    }
