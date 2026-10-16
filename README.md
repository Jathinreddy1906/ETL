# Automated ETL/ELT Pipeline with Apache Airflow

A portfolio-ready data engineering project that extracts JSON data from a REST API, validates records, incrementally loads PostgreSQL, handles failures with retries, records audit logs, and orchestrates tasks using Apache Airflow.

## Architecture

`JSONPlaceholder REST API -> Airflow DAG -> API extraction -> validation -> incremental load/upsert -> PostgreSQL -> SQL reports`

The pipeline uses PostgreSQL for both the target tables and pipeline audit/quality metadata. In this learning project, ELT-style reporting transformations are performed with SQL after the data is loaded.

## Features

- **Workflow orchestration:** Airflow DAG with ordered tasks and a daily schedule.
- **Data validation:** checks required fields, types, and email format; rejected records are counted and captured in quality details.
- **Incremental loading:** uses the highest loaded numeric ID as a watermark for this demo source.
- **Idempotent writes:** PostgreSQL `ON CONFLICT` upserts protect against duplicate inserts during reruns.
- **Error handling:** request timeouts, HTTP status checks, task retries, database transactions, rollback, and logged exceptions.
- **Audit logging:** stores per-entity run status, extracted/loaded/rejected counts, timestamps, and run details.
- **Data-quality tracking:** records validation outcomes in `data_quality_checks`.
- **SQL reporting:** includes join, aggregation, HAVING, and audit queries.

## Requirements

- Docker Desktop with Docker Compose
- Internet access to reach `https://jsonplaceholder.typicode.com`

## Run locally

1. Extract this project folder.
2. Open a terminal in the project directory.
3. Start the services:

   ```bash
   docker compose up --build -d
   ```

4. Open Airflow at [http://localhost:8080](http://localhost:8080).
   - Username: `admin`
   - Password: `admin`

5. Find `api_to_postgres_etl`, enable it, and click **Trigger DAG** for a first run. The DAG is scheduled daily after activation.

6. Connect to PostgreSQL on `localhost:5433` with:
   - Database: `warehouse`
   - Username: `pipeline`
   - Password: `pipeline_pass`

   These are local development credentials only. Do not use them in production.

7. Run the queries in `sql/reports.sql` using pgAdmin, DBeaver, or `psql`.

## Verify the results

```sql
SELECT COUNT(*) FROM users;
SELECT COUNT(*) FROM posts;
SELECT * FROM pipeline_audit ORDER BY audit_id DESC;
SELECT * FROM data_quality_checks ORDER BY check_id DESC;
```

The demo API normally returns 10 users and 100 posts.

## Stop the project

```bash
docker compose down
```

To remove the local PostgreSQL data volume as well:

```bash
docker compose down -v
```

## Important design notes

- JSONPlaceholder is a sample API. Its records have stable numeric IDs but do not provide a reliable `updated_at` field. This project demonstrates **append-style incremental extraction** using the maximum ID and is not a complete change-data-capture solution. Existing records with IDs at or below the watermark are not re-extracted on each run.
- A production pipeline should use a source-provided cursor, modification timestamp, CDC feed, or periodic reconciliation to detect updates and deletions.
- This project keeps the environment local and uses simplified Airflow configuration for learning. For production, use a supported Airflow metadata database, secrets management, restricted credentials, alerting, and deployment-specific executor settings.
- The project demonstrates an ELT reporting stage through SQL queries after loading. The API ingestion task itself validates and maps records before loading.

## Suggested resume bullet

Built an Apache Airflow-orchestrated ETL/ELT pipeline integrating REST API data with PostgreSQL, featuring validation, incremental loading, idempotent upserts, retry-based error handling, audit logging, and SQL reporting.

## Interview explanation

**Problem:** Manual API-to-database data loading is repetitive and difficult to monitor.

**Solution:** An Airflow DAG coordinates schema setup, user ingestion, and post ingestion. Each ingestion task fetches data, applies validation, checks a watermark, loads valid rows transactionally, and writes audit and quality metadata.

**How reliability is handled:** HTTP/database errors are logged and raised so Airflow can retry. PostgreSQL transactions prevent partial commits, while upserts make reruns safe. Audit tables make row counts and failures queryable.
