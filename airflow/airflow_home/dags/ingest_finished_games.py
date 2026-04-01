from airflow.sdk import DAG, task, dag
# from airflow.decorators import dag, task
from db import get_connection, duck_conn
from datetime import datetime, timedelta
import requests
import polars as pl

@dag(
        description="Sincroniza estados de jogos (LIVE/FUT -> OFF) usando dados recentes do Boxscore",
        schedule=timedelta(hours=12),
        start_date=datetime(2026, 3, 26),
        )
def ingest_finished_games():

    @task
    @duck_conn
    def responses_to_games(con):
        query = """
        CREATE OR REPLACE TEMP TABLE tmp_sync_games AS
        WITH sync_games_resp AS (
            SELECT 
                api.endpoint,
                api.requested_at,
                CAST(api.response AS JSON) AS resp_json
            FROM dev.bronze.nhl_api_calls api
            WHERE api.status = 200
              AND api.endpoint LIKE '%/boxscore'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY api.endpoint ORDER BY api.requested_at DESC) = 1
        ),
        new_data AS (
            SELECT
                (resp_json->>'$.id')::BIGINT AS game_id,
                resp_json->>'$.gameState' AS game_state,
                (resp_json->>'$.gameDate')::DATE AS date,
                requested_at
            FROM sync_games_resp
        )
        SELECT 
            n.game_id,
            n.date,
            g.venue,
            g.start_time_utc,
            n.game_state,
            g.game_schedule_state,
            g.away_id,
            g.away_abbrev,
            g.away_common_name,
            g.home_id,
            g.home_abbrev,
            g.home_common_name,
            g.game_type,
            g.endpoint,
            g.endpoint_date,
            n.requested_at
        FROM new_data n 
        INNER JOIN dev.silver.nhl_games g ON g.game_id = n.game_id
        """
        # Executa o MERGE e captura o resultado
        con.execute(query)
        check = con.sql("select count(*) from tmp_sync_games").fetchone()[0]

        if check > 0:
            con.execute("""
                        BEGIN TRANSACTION;

                        DELETE FROM dev.silver.nhl_games 
                        WHERE game_id IN (SELECT game_id FROM tmp_sync_games);

                        INSERT INTO dev.silver.nhl_games SELECT * FROM tmp_sync_games;

                        COMMIT;
                        """)
        
        return {
            "rows_merged": check,
            "status": "success" if check > 0 else "no_updates"
        }

    t1 = responses_to_games()

    @task
    @duck_conn
    def enqueue_pbp(con):
        game_count = con.sql(
                """
                SELECT COUNT(*)
                FROM dev.silver.nhl_games g
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM dev.silver.nhl_play_by_play pbp
                    WHERE g.game_id = pbp.game_id
                    )
                """).fetchone()[0]

        if game_count == 0:
            return {
                    "message": "Sem jogos para nova ingestão"
                    }

        con.execute(
                """
                INSERT INTO dev.bronze.request_queue

                WITH games AS (
                    SELECT game_id
                    FROM dev.silver.nhl_games g
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM dev.silver.nhl_play_by_play pbp
                        WHERE g.game_id = pbp.game_id
                        )
                    ),
                requests AS (
                        SELECT 
                            ('https://api-web.nhle.com/v1/gamecenter/' || game_id || '/play-by-play')
                                AS request
                        FROM games
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM dev.bronze.request_queue q
                            WHERE 
                                q.status = 'pending'
                                AND q.endpoint = request
                            )
                        AND NOT EXISTS (
                            SELECT 1 
                            FROM dev.bronze.nhl_api_calls api
                            WHERE api.endpoint = request
                            AND api.status = 200
                            )
                    )
                SELECT
                    UUID() AS request_id,
                    request AS endpoint, 
                    'pending' AS status,
                    NOW() AS created_at,
                FROM requests
                """
                )
        return {
                "message": "Adicionando play-by-play à fila",
                "game_count": game_count
                }

    t2 = enqueue_pbp()

    t1 >> t2

ingest_finished_games()
