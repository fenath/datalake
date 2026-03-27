from airflow.sdk import DAG, task, dag
# from airflow.decorators import dag, task
from db import get_connection, duck_conn
from datetime import datetime, timedelta
import requests
import polars as pl

@dag(
        schedule=timedelta(minutes=1),
        start_date=datetime(2026, 3, 26),
        )
def request_queue():

    @task
    def process_request_queue():
        with get_connection() as con:
            r = con.sql("""
                SELECT 
                    request_id, 
                    endpoint
                FROM dev.bronze.request_queue WHERE status = 'pending'
                ORDER BY created_at 
                LIMIT 50
            """).fetchall()

            if len(r) == 0:
                return {
                        "processados": 0,
                        "status": "success",
                        }

            rows = []
            for request_id, endpoint in r:
                res = requests.get(endpoint, timeout=30)
                rows.append({
                    "request_id": request_id,
                    "requested_at": datetime.now(),
                    "endpoint": endpoint,
                    "status": res.status_code,
                    "response": res.text    
                })

            request_ids = [str(r_id) for r_id, _ in r]
            con.execute("""UPDATE dev.bronze.request_queue 
                    SET status='done' 
                    WHERE request_id IN 
                    (
                    SELECT CAST(x AS UUID)
                    FROM UNNEST(?) t(x)
                    )""", [request_ids])

            df = pl.DataFrame(rows)[["requested_at", 
                                     "endpoint",
                                     "status",
                                     "response"]]
            con.register("df", df)
            con.execute("""
                        INSERT INTO dev.bronze.nhl_api_calls
                        SELECT * from df
                        """)
            con.unregister("df")

            print(f"{len(rows)} requests atualizados!")

            return {
                    "processados": len(rows),
                    "status": "success",
                    }

    @task
    @duck_conn
    def update_finished_games(con):
        pass

    process_request_queue()

request_queue()
