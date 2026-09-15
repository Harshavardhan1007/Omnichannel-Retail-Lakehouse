"""Operational metadata: tracks each ingestion run and its watermark.

The watermark = the highest `updated_at` we have already ingested for a source.
Next run reads only rows newer than that, which is how incremental loading works.
"""
import os
import psycopg2


def _conn():
    return psycopg2.connect(
        host="localhost", port=5432,
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def ensure_table():
    """Create the ops schema + run-log table if they don't exist (idempotent)."""
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE SCHEMA IF NOT EXISTS ops;
                CREATE TABLE IF NOT EXISTS ops.ingest_runs (
                    run_pk          BIGSERIAL PRIMARY KEY,
                    source_id       TEXT NOT NULL,
                    run_id          TEXT NOT NULL,
                    status          TEXT NOT NULL,        -- SUCCESS | FAILED
                    load_type       TEXT,
                    rows_ingested   BIGINT,
                    watermark_value TIMESTAMP,            -- max updated_at seen this run
                    finished_at     TIMESTAMP DEFAULT now()
                );
                """
            )
        conn.commit()
    finally:
        conn.close()


def get_last_watermark(source_id):
    """Highest watermark from previous SUCCESSFUL runs, or None if never run."""
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT MAX(watermark_value) FROM ops.ingest_runs
                WHERE source_id = %s AND status = 'SUCCESS'
                """,
                (source_id,),
            )
            return cur.fetchone()[0]
    finally:
        conn.close()


def record_run(source_id, run_id, status, load_type, rows, watermark):
    """Write one row into the run log."""
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.ingest_runs
                    (source_id, run_id, status, load_type, rows_ingested, watermark_value)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (source_id, run_id, status, load_type, rows, watermark),
            )
        conn.commit()
    finally:
        conn.close()
