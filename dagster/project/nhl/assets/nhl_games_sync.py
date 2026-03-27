from project.resources import IcebergDuckDBResource
from typing import Any, reveal_type
import dagster as dg
import polars as pl
import uuid
from datetime import datetime

@dg.asset(
        description="Lista os jogos passados com state não finalizado e faz nova busca (queue)",
        key_prefix=["bronze", "reload_game_info"]
        )
def reload_game_info(context: dg.AssetExecutionContext,
      duckdb: IcebergDuckDBResource
      ):
    query = """
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
    with duckdb.get_connection() as con:
        duckdb.post_connect(con)
        con.execute(query)
        res = con.sql(
                'SELECT count(*), list(DISTINCT endpoint) FROM tmp_sync_nhl_games'
                ).fetchone()

        game_count = res[0]
        game_ids = res[1]

        if game_count == 0:
            context.log.info("Não há jogos para se atualizar")
            return

        con.execute("INSERT INTO dev.bronze.request_queue SELECT * FROM tmp_sync_nhl_games")

        context.log.info(f"{game_count} jogos adicionados para sincronização de dados")
        context.add_output_metadata({
            "game_count": game_count,
            "game_ids": game_ids,
        })
        return game_ids

@dg.asset(
        description="Sincroniza estados de jogos (LIVE/FUT -> OFF) usando dados recentes do Boxscore",
        key_prefix=["silver"], # Sugestão: mover para silver pois ele altera a silver
        name="sync_nhl_game_states"
        )
def sync_nhl_game_states(context: dg.AssetExecutionContext,
      duckdb: IcebergDuckDBResource
      ):
    with duckdb.get_connection() as con:
        duckdb.post_connect(con)

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
                        DELETE FROM dev.silver.nhl_games 
                        WHERE game_id IN (SELECT game_id FROM tmp_sync_games)
                        """)
            con.execute("""
                        INSERT INTO dev.silver.nhl_games SELECT * FROM tmp_sync_games
                        """)
        
        context.add_output_metadata({
            "rows_merged": check,
            "status": "success" if check > 0 else "no_updates"
        })

@dg.asset(
        description="Busca jogos finalizados que ainda não possuem pbp e insere na fila",
        key_prefix=["bronze"], 
        name="request_for_pbps"
        )
def request_for_pbps(context: dg.AssetExecutionContext,
      duckdb: IcebergDuckDBResource
      ):
    with duckdb.get_connection() as con:
        duckdb.post_connect(con)
        query = """"""

