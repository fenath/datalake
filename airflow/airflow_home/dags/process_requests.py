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
            request_ids = [str(r_id) for r_id, _ in r]
            con.execute("""UPDATE dev.bronze.request_queue 
                    SET status='done' 
                    WHERE request_id IN 
                    (
                    SELECT CAST(x AS UUID)
                    FROM UNNEST(?) t(x)
                    )""", [request_ids])

            print(f"{len(rows)} requests atualizados!")

            return {
                    "processados": len(rows),
                    "status": "success",
                    }

    process_request_queue()

@dag(schedule=timedelta(days=1),
     start_date=datetime(2026, 3, 26, 3),
     )
def update_finished_games():
    @task
    @duck_conn
    def queue_outdated_games(con):
        con.execute(
            """
            CREATE or REPLACE temp table tmp_sync_nhl_games AS
            SELECT 
                uuid() AS request_id,
                ('https://api-web.nhle.com/v1/gamecenter/' ||game_id|| '/boxscore') AS endpoint,
                'pending' AS status,
                now() AS created_at
            FROM dev.silver.nhl_games 
            WHERE game_state IN ( 'LIVE', 'FUT' )
            AND start_time_utc < (now() - INTERVAL '5 hours')
            AND NOT EXISTS (
                SELECT 1 FROM dev.bronze.request_queue q
                WHERE q.status = 'pending'
                AND q.endpoint = ('https://api-web.nhle.com/v1/gamecenter/' ||game_id|| '/boxscore')
            )
            """
        )
        res = con.sql(
                'SELECT count(*), list(DISTINCT endpoint) FROM tmp_sync_nhl_games'
                ).fetchone()
        game_count = res[0]
        game_ids = res[1]

        if game_count == 0:
            return {
                    "message": 'Não há jogos a atualizar',
                    "games_to_update": 0
                    }

        con.execute("""
                    INSERT INTO dev.bronze.request_queue
                    SELECT * FROM tmp_sync_nhl_games
                    """)

        return {
                "message": f"{game_count} jogos desatualizados, adicionados à fila.",
                "game_count": game_count,
                "games": game_ids,
                }
    queue_outdated_games()


@dag(schedule=timedelta(days=7),
     start_date=datetime(2026, 3, 26, 3),
     )
def request_game_week():
    @task
    @duck_conn
    def queue_game_week(con):
        tmp = """
        CREATE OR REPLACE TEMP TABLE temp_novos_jogos AS (
            SELECT 
                api.endpoint,
                api.requested_at,
                CAST(api.response AS JSON) as resp_json
            FROM dev.bronze.nhl_api_calls api
            WHERE api.status = 200
              AND api.endpoint LIKE 'https://api-web.nhle.com/v1/schedule/%'
              AND NOT EXISTS (
                  SELECT 1 FROM dev.silver.nhl_games g 
                  WHERE g.endpoint = api.endpoint
              )
            QUALIFY ROW_NUMBER() OVER (PARTITION BY api.endpoint ORDER BY api.requested_at DESC) = 1
        )
        """
        con.execute(tmp)

        query = """
        INSERT INTO dev.silver.nhl_games
        WITH flattened_weeks AS (
            SELECT 
                endpoint,
                requested_at,
                unnest(CAST(resp_json->'$.gameWeek' AS JSON[])) as week
            FROM temp_novos_jogos
        ),
        flattened_games AS (
            SELECT 
                endpoint,
                requested_at,
                unnest(CAST(week->'$.games' AS JSON[])) as g,
                regexp_extract(endpoint, '/schedule/(\d{4}-\d{2}-\d{2})', 1)::DATE as endpoint_date
            FROM flattened_weeks
        )
        SELECT 
            (g->>'$.id')::BIGINT as game_id,
            (g->>'$.gameDate')::DATE as date,
            g->'$.venue'->>'$.default' as venue,
            (g->>'$.startTimeUTC')::TIMESTAMP as start_time_utc,
            g->>'$.gameState' as game_state,
            g->>'$.gameScheduleState' as game_schedule_state,
            (g->'$.awayTeam'->>'$.id')::INT as away_id,
            g->'$.awayTeam'->>'$.abbrev' as away_abbrev,
            g->'$.awayTeam'->'$.commonName'->>'$.default' as away_common_name,
            (g->'$.homeTeam'->>'$.id')::INT as home_id,
            g->'$.homeTeam'->>'$.abbrev' as home_abbrev,
            g->'$.homeTeam'->'$.commonName'->>'$.default' as home_common_name,
            (g->>'$.gameType')::INT as game_type,
            endpoint,
            endpoint_date,
            requested_at
        FROM flattened_games
        WHERE (g->>'$.gameType')::INT <= 3
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY (g->>'$.id') -- Se o ID do jogo for igual...
            ORDER BY requested_at DESC -- ...mantenha apenas o da carga mais nova
        ) = 1
        """
        
        con.execute(query)

        # 2. Obter metadados para o Dagster (Quais jogos foram ingeridos?)
        # Consultamos a própria tabela que acabamos de inserir usando o timestamp da execução
        res = con.sql(f"""
            SELECT count(*), list(distinct game_id) 
            FROM dev.silver.nhl_games 
            WHERE requested_at IN (SELECT max(requested_at) FROM temp_novos_jogos)
        """).fetchone()

        num_rows = res[0]
        game_ids = res[1]

        # Log do evento com os IDs dos jogos para visibilidade no Dagster
        return {
                    "num_rows": num_rows,
                    "ingested_game_ids": game_ids,
                    "engine": "DuckDB Native (No Polars)"
                }

    queue_game_week()

request_queue()
update_finished_games()
request_game_week()
