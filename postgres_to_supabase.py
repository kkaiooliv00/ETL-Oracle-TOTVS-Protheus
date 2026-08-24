"""Carga PostgreSQL de origem para o Supabase."""

from __future__ import annotations

import argparse
import logging
import os
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import quote as url_quote

import pandas as pd
import psycopg
import yaml
from dotenv import load_dotenv
from pandas.api.types import is_object_dtype, is_string_dtype
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.pool import QueuePool

load_dotenv()

GW_DATABASE_URL_ENV = "GW_DATABASE_URL"
DATABASE_URL_ENV = "DATABASE_URL"
GW_SOURCE_HOST_ENV = "GW_SOURCE_HOST"
GW_SOURCE_PORT_ENV = "GW_SOURCE_PORT"
GW_SOURCE_DATABASE_ENV = "GW_SOURCE_DATABASE"
GW_SOURCE_USER_ENV = "GW_SOURCE_USER"
GW_SOURCE_PASSWORD_ENV = "GW_SOURCE_PASSWORD"
GW_SOURCE_SSLMODE_ENV = "GW_SOURCE_SSLMODE"
JOBS_FILE = Path(os.getenv("POSTGRES_JOBS_FILE", Path(__file__).parent / "postgres_jobs.yml"))


def positive_int_env(name: str, default: int) -> int:
    raw_value = os.getenv(name, str(default))
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise RuntimeError(f"{name} deve ser um numero inteiro positivo.") from exc
    if value <= 0:
        raise RuntimeError(f"{name} deve ser maior que zero.")
    return value


FETCH_SIZE = positive_int_env("POSTGRES_FETCH_SIZE", 20_000)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("postgres_to_supabase")


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Variavel de ambiente obrigatoria nao definida: {name}")
    return value


def source_database_url() -> str:
    """Monta a URL da origem sem expor a senha em arquivos ou logs."""
    host = require_env(GW_SOURCE_HOST_ENV)
    port = require_env(GW_SOURCE_PORT_ENV)
    database = require_env(GW_SOURCE_DATABASE_ENV)
    user = require_env(GW_SOURCE_USER_ENV)
    password = require_env(GW_SOURCE_PASSWORD_ENV)
    sslmode = os.getenv(GW_SOURCE_SSLMODE_ENV, "require")
    return (
        f"postgresql://{url_quote(user, safe='')}:{url_quote(password, safe='')}"
        f"@{host}:{port}/{url_quote(database, safe='')}?sslmode={url_quote(sslmode, safe='')}"
    )


def destination_database_url() -> str:
    """Usa a conexao GW exclusiva quando existir; senao, a do Supabase atual."""
    return os.getenv(GW_DATABASE_URL_ENV) or require_env(DATABASE_URL_ENV)


def sqlalchemy_database_url(database_url: str) -> str:
    """Forca o driver psycopg v3, que e a dependencia usada pelo projeto."""
    if database_url.startswith("postgresql://"):
        return database_url.replace("postgresql://", "postgresql+psycopg://", 1)
    if database_url.startswith("postgres://"):
        return database_url.replace("postgres://", "postgresql+psycopg://", 1)
    return database_url


def quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def table_name(schema: str, table: str) -> str:
    return f"{quote(schema)}.{quote(table)}"


def load_jobs() -> list[dict[str, str]]:
    with JOBS_FILE.open(encoding="utf-8") as file:
        config = yaml.safe_load(file) or {}
    jobs = config.get("jobs")
    if not isinstance(jobs, list) or not jobs:
        raise RuntimeError("postgres_jobs.yml deve conter uma lista nao vazia em jobs.")

    loaded: list[dict[str, str]] = []
    for job in jobs:
        if not isinstance(job, dict):
            raise RuntimeError("Cada job PostgreSQL deve ser um objeto.")
        schema = str(job.get("target_schema", "")).strip()
        target = str(job.get("target_table", "")).strip()
        key = str(job.get("business_key", "super_chave")).strip()
        query_file = str(job.get("query_file", "")).strip()
        if not all((schema, target, key, query_file)):
            raise RuntimeError("Job PostgreSQL requer target_schema, target_table, business_key e query_file.")
        query_path = JOBS_FILE.parent / query_file
        if not query_path.exists():
            raise RuntimeError(f"Query nao encontrada: {query_path}")
        loaded.append({
            "target_schema": schema,
            "target_table": target,
            "business_key": key,
            "query": query_path.read_text(encoding="utf-8").strip(),
        })
    return loaded


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    for column in df.columns:
        if is_object_dtype(df[column]) or is_string_dtype(df[column]):
            df[column] = df[column].apply(
                lambda value: value.replace("\x00", "").replace("\xa0", " ").strip()
                if isinstance(value, str) else value
            )
    return df


def postgres_type(series: pd.Series) -> str:
    dtype = str(series.dtype)
    if "bool" in dtype:
        return "BOOLEAN"
    if "int" in dtype:
        return "BIGINT"
    if "float" in dtype:
        return "NUMERIC"
    if "datetime" in dtype:
        return "TIMESTAMPTZ"
    sample = next((value for value in series if value is not None and not pd.isna(value)), None)
    if isinstance(sample, Decimal):
        return "NUMERIC"
    if isinstance(sample, datetime):
        return "TIMESTAMPTZ"
    if isinstance(sample, date):
        return "DATE"
    return "TEXT"


def copy_to_staging(destination_url: str, schema: str, staging: str, df: pd.DataFrame, replace: bool) -> None:
    if df.empty:
        return
    columns = [str(column) for column in df.columns]
    staging_fqn = table_name(schema, staging)
    with psycopg.connect(destination_url) as connection:
        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {quote(schema)}")
        if replace:
            definitions = ", ".join(
                f"{quote(column)} {postgres_type(df[column])}" for column in columns
            )
            connection.execute(f"DROP TABLE IF EXISTS {staging_fqn}")
            connection.execute(f"CREATE TABLE {staging_fqn} ({definitions})")
        columns_sql = ", ".join(quote(column) for column in columns)
        with connection.cursor() as cursor:
            with cursor.copy(f"COPY {staging_fqn} ({columns_sql}) FROM STDIN") as copy:
                for row in df.itertuples(index=False, name=None):
                    copy.write_row([None if pd.isna(value) else value for value in row])
        connection.commit()


def add_missing_columns(connection: Connection, engine: Engine, schema: str, target: str, staging: str) -> None:
    inspector = inspect(connection)
    target_columns = {column["name"] for column in inspector.get_columns(target, schema=schema)}
    for column in inspector.get_columns(staging, schema=schema):
        if column["name"] not in target_columns:
            column_type = column["type"].compile(dialect=engine.dialect)
            connection.execute(text(
                f"ALTER TABLE {table_name(schema, target)} "
                f"ADD COLUMN IF NOT EXISTS {quote(column['name'])} {column_type}"
            ))


def finalize_load(engine: Engine, schema: str, target: str, key: str, columns: list[str]) -> None:
    staging = f"{target}_staging"
    dedup = f"{target}_staging_dedup"
    target_fqn = table_name(schema, target)
    staging_fqn = table_name(schema, staging)
    dedup_fqn = table_name(schema, dedup)
    columns_sql = ", ".join(quote(column) for column in columns)
    updates = ", ".join(
        f"{quote(column)} = EXCLUDED.{quote(column)}" for column in columns if column != key
    )
    index_name = f"{target}_{key}_unique"[:63]

    with engine.begin() as connection:
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {quote(schema)}"))
        connection.execute(text(f"CREATE TABLE IF NOT EXISTS {target_fqn} AS TABLE {staging_fqn} WITH NO DATA"))
        add_missing_columns(connection, engine, schema, target, staging)
        connection.execute(text(
            f"DELETE FROM {target_fqn} older USING {target_fqn} newer "
            f"WHERE older.ctid < newer.ctid AND older.{quote(key)} = newer.{quote(key)}"
        ))
        connection.execute(text(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {quote(index_name)} ON {target_fqn} ({quote(key)})"
        ))
        connection.execute(text(f"DROP TABLE IF EXISTS {dedup_fqn}"))
        connection.execute(text(
            f"CREATE TABLE {dedup_fqn} AS "
            f"SELECT DISTINCT ON ({quote(key)}) {columns_sql} FROM {staging_fqn} "
            f"ORDER BY {quote(key)}, ctid DESC"
        ))
        action = f"DO UPDATE SET {updates}" if updates else "DO NOTHING"
        connection.execute(text(
            f"INSERT INTO {target_fqn} ({columns_sql}) SELECT {columns_sql} FROM {dedup_fqn} "
            f"ON CONFLICT ({quote(key)}) {action}"
        ))
        connection.execute(text(f"DROP TABLE IF EXISTS {dedup_fqn}"))
        connection.execute(text(f"DROP TABLE IF EXISTS {staging_fqn}"))


def run_job(source_url: str, destination_url: str, job: dict[str, str]) -> None:
    schema, target, key, query = (job["target_schema"], job["target_table"], job["business_key"], job["query"])
    staging = f"{target}_staging"
    destination_engine = create_engine(
        sqlalchemy_database_url(destination_url),
        poolclass=QueuePool,
        pool_size=2,
        max_overflow=0,
        pool_pre_ping=True,
    )
    rows = 0
    first_batch = True
    columns: list[str] | None = None
    try:
        with psycopg.connect(source_url) as source_connection:
            with source_connection.cursor(name=f"{target}_export") as cursor:
                cursor.execute(query)
                column_names = [description.name.lower() for description in cursor.description]
                while records := cursor.fetchmany(FETCH_SIZE):
                    df = normalize_dataframe(pd.DataFrame.from_records(records, columns=column_names))
                    columns = [str(column) for column in df.columns]
                    copy_to_staging(destination_url, schema, staging, df, replace=first_batch)
                    first_batch = False
                    rows += len(df)
                    logger.info("%s | %s registros copiados para staging.", target, rows)
        if first_batch or columns is None:
            logger.info("%s | Nenhum registro retornado; carga dispensada.", target)
            return
        finalize_load(destination_engine, schema, target, key, columns)
        logger.info("%s | Carga concluida em %s.%s: %s registros.", target, schema, target, rows)
    finally:
        destination_engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description="Carga PostgreSQL de origem para Supabase.")
    parser.add_argument("--tables", default="", help="Tabelas destino separadas por virgula.")
    args = parser.parse_args()
    selected = {item.strip().lower() for item in args.tables.split(",") if item.strip()}
    jobs = [job for job in load_jobs() if not selected or job["target_table"].lower() in selected]
    if not jobs:
        raise RuntimeError("Nenhum job PostgreSQL selecionado.")
    source_url = source_database_url()
    destination_url = destination_database_url()
    for job in jobs:
        run_job(source_url, destination_url, job)


if __name__ == "__main__":
    main()
