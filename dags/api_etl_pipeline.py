from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

import psycopg2
import requests
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators.python import PythonOperator

LOG = logging.getLogger(__name__)
API_BASE_URL = os.getenv("SOURCE_API_BASE_URL", "https://jsonplaceholder.typicode.com")
DB_CONFIG = {
    "host": os.getenv("WAREHOUSE_HOST", "localhost"),
    "port": int(os.getenv("WAREHOUSE_PORT", "5432")),
    "dbname": os.getenv("WAREHOUSE_DB", "warehouse"),
    "user": os.getenv("WAREHOUSE_USER", "pipeline"),
    "password": os.getenv("WAREHOUSE_PASSWORD", "pipeline_pass"),
    "connect_timeout": 10,
}


def db_connect():
    return psycopg2.connect(**DB_CONFIG)


def initialize_schema():
    """Create target, watermark, and audit tables if they do not exist."""
    ddl = """
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        username TEXT NOT NULL,
        email TEXT NOT NULL,
        city TEXT,
        source_updated_at TIMESTAMPTZ,
        loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS posts (
        id INTEGER PRIMARY KEY,
        user_id INTEGER NOT NULL,
        title TEXT NOT NULL,
        body TEXT NOT NULL,
        source_updated_at TIMESTAMPTZ,
        loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS pipeline_audit (
        audit_id BIGSERIAL PRIMARY KEY,
        dag_run_id TEXT,
        entity_name TEXT NOT NULL,
        status TEXT NOT NULL,
        extracted_count INTEGER NOT NULL DEFAULT 0,
        inserted_or_updated_count INTEGER NOT NULL DEFAULT 0,
        rejected_count INTEGER NOT NULL DEFAULT 0,
        started_at TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ,
        message TEXT,
        details JSONB
    );

    CREATE TABLE IF NOT EXISTS data_quality_checks (
        check_id BIGSERIAL PRIMARY KEY,
        dag_run_id TEXT,
        entity_name TEXT NOT NULL,
        check_name TEXT NOT NULL,
        passed BOOLEAN NOT NULL,
        checked_count INTEGER NOT NULL,
        failed_count INTEGER NOT NULL,
        checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        details JSONB
    );
    """
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
    LOG.info("Target schema is ready.")


def fetch_json(endpoint: str):
    url = f"{API_BASE_URL.rstrip('/')}/{endpoint.lstrip('/')}"
    response = requests.get(url, timeout=(5, 30))
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError(f"Expected a JSON array from {url}")
    return payload


def get_watermark(table_name: str) -> int:
    # IDs are the incremental watermark for this demo API.
    if table_name not in {"users", "posts"}:
        raise ValueError("Unsupported table name")
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COALESCE(MAX(id), 0) FROM {table_name}")
            return int(cur.fetchone()[0])


def validate_users(rows):
    valid, rejected = [], []
    for row in rows:
        errors = []
        if not isinstance(row.get("id"), int):
            errors.append("id must be an integer")
        if not row.get("name"):
            errors.append("name is required")
        if not row.get("username"):
            errors.append("username is required")
        if not row.get("email") or "@" not in str(row.get("email")):
            errors.append("valid email is required")
        if errors:
            rejected.append({"record": row, "errors": errors})
        else:
            address = row.get("address") or {}
            valid.append((
                row["id"], row["name"], row["username"], row["email"],
                (address.get("city") if isinstance(address, dict) else None),
                None
            ))
    return valid, rejected


def validate_posts(rows):
    valid, rejected = [], []
    for row in rows:
        errors = []
        if not isinstance(row.get("id"), int):
            errors.append("id must be an integer")
        if not isinstance(row.get("userId"), int):
            errors.append("userId must be an integer")
        if not row.get("title"):
            errors.append("title is required")
        if not row.get("body"):
            errors.append("body is required")
        if errors:
            rejected.append({"record": row, "errors": errors})
        else:
            valid.append((row["id"], row["userId"], row["title"], row["body"], None))
    return valid, rejected


def write_audit(cur, dag_run_id, entity, status, extracted, loaded, rejected,
                started, message=None, details=None):
    cur.execute(
        """
        INSERT INTO pipeline_audit
        (dag_run_id, entity_name, status, extracted_count,
         inserted_or_updated_count, rejected_count, started_at, finished_at,
         message, details)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (dag_run_id, entity, status, extracted, loaded, rejected, started,
         datetime.now(timezone.utc), message, json.dumps(details or {}))
    )


def load_entity(entity: str, ti):
    started = datetime.now(timezone.utc)
    dag_run_id = ti.run_id
    table = entity
    extracted = loaded = 0
    rejected_rows = []
    try:
        raw = fetch_json(entity)
        extracted = len(raw)
        watermark = get_watermark(table)
        # The demo API has stable numeric IDs and no updated_at field. This
        # demonstrates append-style incremental loading; upserts make reruns safe.
        incremental = [r for r in raw if isinstance(r.get("id"), int) and r["id"] > watermark]
        if entity == "users":
            valid_rows, rejected_rows = validate_users(incremental)
            insert_sql = """
                INSERT INTO users (id,name,username,email,city,source_updated_at)
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                  name=EXCLUDED.name, username=EXCLUDED.username,
                  email=EXCLUDED.email, city=EXCLUDED.city, loaded_at=NOW()
            """
        else:
            valid_rows, rejected_rows = validate_posts(incremental)
            insert_sql = """
                INSERT INTO posts (id,user_id,title,body,source_updated_at)
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                  user_id=EXCLUDED.user_id, title=EXCLUDED.title,
                  body=EXCLUDED.body, loaded_at=NOW()
            """
        with db_connect() as conn:
            with conn.cursor() as cur:
                if valid_rows:
                    execute_values(cur, insert_sql, valid_rows)
                    loaded = len(valid_rows)
                cur.execute(
                    """
                    INSERT INTO data_quality_checks
                    (dag_run_id, entity_name, check_name, passed, checked_count,
                     failed_count, details)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (dag_run_id, entity, "required_fields_and_types",
                     len(rejected_rows) == 0, len(incremental), len(rejected_rows),
                     json.dumps(rejected_rows[:20]))
                )
                write_audit(
                    cur, dag_run_id, entity,
                    "SUCCESS" if not rejected_rows else "SUCCESS_WITH_REJECTS",
                    extracted, loaded, len(rejected_rows), started,
                    f"Watermark={watermark}; incremental_rows={len(incremental)}",
                    {"watermark": watermark, "incremental_count": len(incremental)}
                )
        LOG.info("%s: extracted=%s incremental=%s loaded=%s rejected=%s",
                 entity, extracted, len(incremental), loaded, len(rejected_rows))
        return {"entity": entity, "extracted": extracted,
                "incremental": len(incremental), "loaded": loaded,
                "rejected": len(rejected_rows)}
    except Exception as exc:
        LOG.exception("Pipeline failed for entity %s", entity)
        # Record failure separately so that the original exception still fails the task.
        try:
            with db_connect() as conn:
                with conn.cursor() as cur:
                    write_audit(cur, dag_run_id, entity, "FAILED", extracted, loaded,
                                len(rejected_rows), started, str(exc))
        except Exception:
            LOG.exception("Could not write failure audit record")
        raise


def load_users(**context):
    return load_entity("users", context["ti"])


def load_posts(**context):
    return load_entity("posts", context["ti"])


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": __import__("datetime").timedelta(minutes=2),
}

with DAG(
    dag_id="api_to_postgres_etl",
    description="Validated, incremental API ingestion with audit logging",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["etl", "postgres", "api"],
) as dag:
    setup = PythonOperator(task_id="initialize_schema", python_callable=initialize_schema)
    users = PythonOperator(task_id="load_users_incrementally", python_callable=load_users)
    posts = PythonOperator(task_id="load_posts_incrementally", python_callable=load_posts)
    setup >> users >> posts
