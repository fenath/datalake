"""
### DuckDB tutorial DAG 2

This DAG shows how to use the DuckDBHook in an Airflow task.
"""

# from airflow.decorators import dag, task
from airflow.sdk import DAG
from pendulum import datetime
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from datetime import timedelta

DUCKDB_CONN_ID = "duckdb"
DUCKDB_TABLE_NAME = "dev.bronze.request_queue"

from db import get_connection

with DAG(
        "test_duckdb",
        description="Teste do duckdb",
        schedule=timedelta(days=1),
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=["example"], # type: ignore
        ) as dag:

    conn_id = DUCKDB_CONN_ID
    my_table = DUCKDB_TABLE_NAME
    
    hook = DuckDBHook.get_hook(conn_id)
    con = hook.get_conn()

    with get_connection() as con:
        r = con.execute(f"select * from {my_table};").fetchall()
        print(r)
