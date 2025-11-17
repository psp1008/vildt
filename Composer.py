from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime
import asyncio
import aiohttp
import os


# ----------------------------------------
# Read SQL from file
# ----------------------------------------
SQL_FILE = os.path.join(os.path.dirname(__file__), "sql/fetch_features.sql")


def load_sql():
    with open(SQL_FILE, "r") as f:
        return f.read()


API_URL = "https://your-api-endpoint.com/ingest"


# ----------------------------------------
# 1. Load BQ data into list of dicts
# ----------------------------------------
@task
def load_from_bq():
    query = load_sql()

    hook = BigQueryHook()
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]

    result = [dict(zip(columns, row)) for row in rows]

    log = LoggingMixin().log
    log.info(f"Loaded {len(result)} rows from BigQuery")

    return result


# ----------------------------------------
# 2. Async API call
# ----------------------------------------
async def call_api(session, payload, log):
    try:
        async with session.post(API_URL, json=payload) as resp:
            text = await resp.text()

            if resp.status != 200:
                log.error(f"API FAILED status={resp.status} response={text}")
            else:
                log.info(f"API SUCCESS response={text}")

    except Exception:
        log.exception("API call failed")


# ----------------------------------------
# 3. Process rows asynchronously
# ----------------------------------------
async def process_rows_async(rows, log):
    tasks = []
    async with aiohttp.ClientSession() as session:
        for row in rows:
            payload = {
                "feature_id": row["feature_id"],
                "device_key": row["device_key"],
                "timestamp": None
            }

            tasks.append(asyncio.ensure_future(call_api(session, payload, log)))

        await asyncio.gather(*tasks)


# ----------------------------------------
# 4. Airflow wrapper task
# ----------------------------------------
@task
def process_rows(rows):
    log = LoggingMixin().log
    log.info(f"Processing {len(rows)} rows asynchronously...")

    asyncio.run(process_rows_async(rows, log))


# ----------------------------------------
# DAG Definition
# ----------------------------------------
with DAG(
    "bq_async_api_ingestion",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["featurestore", "bq", "async"]
):
    rows = load_from_bq()
    process_rows(rows)
