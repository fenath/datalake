from project.resources import IcebergDuckDBResource
from typing import Any
import requests
from datetime import datetime

import dagster as dg
import polars as pl

@dg.asset(
        kinds={"duckdb"}
        )
def request_queue_table(duckdb: IcebergDuckDBResource):
    with duckdb.get_connection() as con:
        con.sql("""
            CREATE TABLE IF NOT EXISTS dev.bronze.request_queue (
                request_id UUID PRIMARY KEY, -- Boa prática ter PK
                endpoint TEXT UNIQUE,        -- Aqui a mágica acontece
                status TEXT,
                created_at TIMESTAMP
            )
        """)


@dg.asset(
        kinds={"duckdb", "polars", "python", "iceberg"},
        key=["bronze", "nhl_api_calls"],
        deps=[request_queue_table],
        automation_condition=dg.AutomationCondition.on_cron(
            "* * * * *" #every minute
            )
        )
def nhl_api_calls(
        context: dg.AssetExecutionContext,
        duckdb: IcebergDuckDBResource,
        iceberg_catalog: dg.ResourceParam[Any]
        ):
    to_update = []
    cat = iceberg_catalog
    with duckdb.get_connection() as con:
        duckdb.post_connect(con)
        df = con.sql("""
            SELECT 
                request_id, 
                endpoint
            FROM dev.bronze.request_queue WHERE status = 'pending'
            ORDER BY created_at 
            LIMIT 50
        """).pl()
        api_calls = cat.load_table('bronze.nhl_api_calls')
        schema = api_calls.schema().as_arrow()
        rows = []
        for request_id, endpoint in df.rows():
            response = requests.get(endpoint, timeout=30)
            to_update.append(request_id)
            rows.append({
                "requested_at": datetime.now(),
                "endpoint": endpoint,
                "status": response.status_code,
                "response": response.text    
            })
            
        if df.is_empty():
            context.log.info("Fila vazia. Nada a processar.")
            return

        df_rows = pl.DataFrame(rows)
        df_rows.write_iceberg(api_calls, mode="append")
        con.execute("""UPDATE dev.bronze.request_queue 
                SET status='done' 
                WHERE request_id IN 
                (
                SELECT CAST(x AS UUID)
                FROM UNNEST(?) t(x)
                )""", [to_update])
        context.log.info(f"{len(to_update)} requests atualizados!")
    
