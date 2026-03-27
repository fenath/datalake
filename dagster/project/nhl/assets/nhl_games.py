from project.resources import IcebergDuckDBResource
from typing import Any, reveal_type
import dagster as dg
import polars as pl
import uuid
from datetime import datetime

@dg.asset(
        description="Processa o calendário da NHL da Bronze para a Silver via DuckDB Iceberg",
        metadata={"storage": "Iceberg"}
        )
def game_schedule(context: dg.AssetExecutionContext,
                  duckdb: IcebergDuckDBResource,
                  iceberg_catalog: dg.ResourceParam[Any]):
    cat = iceberg_catalog
    with duckdb.get_connection() as con:
        duckdb.post_connect(con)

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
    context.log_event(
        dg.AssetMaterialization(
            asset_key="game_schedule",
            metadata={
                "num_rows": num_rows,
                "ingested_game_ids": dg.MetadataValue.json(game_ids),
                "engine": "DuckDB Native (No Polars)"
            }
        )
    )
    return game_ids

@dg.asset
def play_by_play(context: dg.AssetExecutionContext,
                 duckdb: IcebergDuckDBResource):
    with duckdb.get_connection() as con:
        duckdb.post_connect(con)

        # jogos sem pbp:
        con.execute("""
                    CREATE OR REPLACE TEMP TABLE tmp_no_pbp AS
                    SELECT DISTINCT g.game_id
                    FROM dev.silver.nhl_games g
                    LEFT JOIN dev.silver.nhl_play_by_play pbp ON g.game_id = pbp.game_id
                    WHERE g.date < today()
                        AND g.game_state IN ('FINAL', 'OFF')
                        AND pbp.game_id IS NOT NULL
                    """)

        # buscar respostas na bronze:
        con.execute("""
                    CREATE OR REPLACE TEMP TABLE tmp_pbp_responses AS
                    SELECT
                        m.game_id,
                        api.status,
                        api.response->'$.plays' AS plays,
                        (api.response->>'$.gameDate')::TIMESTAMP AS game_date,
                        api.response->'$.homeTeam' AS home_team_id
                    FROM tmp_no_pbp m
                    JOIN dev.bronze.nhl_api_calls api 
                        ON api.endpoint = ('https://api-web.nhle.com/v1/gamecenter/' || m.game_id || '/play-by-play')
                    WHERE api.status = 200
                    """)

        current_batch_id = str(uuid.uuid4())
        pbp_query = f"""
                CREATE OR REPLACE TEMP TABLE tmp_pbp_transformed AS
                WITH base_responses AS (
                    SELECT 
                        game_id, 
                        CAST(plays AS JSON[]) as plays_array, -- Prepara para o unnest
                        home_team_id::BIGINT as home_team_id, 
                        game_date
                    FROM tmp_pbp_responses
                    WHERE status = 200
                ),
                exploded_plays AS (
                    SELECT 
                        game_id,
                        home_team_id,
                        game_date,
                        unnest(plays_array) as p
                    FROM base_responses
                ),
                extracted_fields AS (
                    SELECT
                        '{current_batch_id}' as batch_id,
                        game_id,
                        home_team_id,
                        game_date,
                        (p->>'$.eventId')::BIGINT as event_id,
                        (p->'$.periodDescriptor'->>'$.number')::INT as period,
                        p->>'$.timeInPeriod' as time_period,
                        (p->>'$.situationCode') as situation_code,
                        p->>'$.homeTeamDefendingSide' as home_team_defending_side,
                        p->>'$.typeCode' as type_code,
                        p->>'$.typeDescKey' as type_desc,
                        (p->>'$.sortOrder')::INT as sort_order,
                        -- Detalhes aninhados
                        (p->'$.details'->>'$.xCoord')::DOUBLE as x_coord,
                        (p->'$.details'->>'$.yCoord')::DOUBLE as y_coord,
                        p->'$.details'->>'$.zoneCode' as zone_code,
                        (p->'$.details'->>'$.eventOwnerTeamId')::BIGINT as event_owner_team_id,
                        -- Split do tempo (Minuto:Segundo)
                        string_split(p->>'$.timeInPeriod', ':') as t_split
                    FROM exploded_plays
                )
                SELECT 
                    batch_id, game_id, game_date, event_id, period, time_period,
                    t_split[1]::INT as time_minute,
                    t_split[2]::INT as time_second,
                    situation_code, home_team_defending_side, type_code,
                    type_desc, sort_order, x_coord, y_coord,
                    -- Lógica de Normalização de Coordenadas (X_NORMALIZED)
                    CASE 
                        WHEN (event_owner_team_id = home_team_id AND home_team_defending_side = 'right')
                          OR (event_owner_team_id != home_team_id AND home_team_defending_side = 'left')
                        THEN x_coord * -1
                        ELSE x_coord
                    END as x_normalized,
                    zone_code, event_owner_team_id
                FROM extracted_fields
                """
        con.execute(pbp_query)

        check = con.sql("SELECT COUNT(*) FROM tmp_pbp_transformed").fetchone()[0]

        if check == 0:
            context.log.info(f"Sem eventos para ingestar.")
            return

        if check > 0:
            con.execute("INSERT INTO dev.silver.nhl_play_by_play SELECT * FROM tmp_pbp_transformed")
            # Registro do Batch para Auditoria
            con.execute(f"""
                INSERT INTO dev.bronze.ingestion (
                    batch_id, 
                    event_time, 
                    source_table, 
                    target_table, 
                    final_status,
                    event_type
                )
                VALUES (
                    '{current_batch_id}', 
                    now(), 
                    'bronze.nhl_api_calls', 
                    'silver.nhl_play_by_play', 
                    'SUCCESS',
                    'APPEND_SYNC'
                )
            """)
            
            # Salvar os parâmetros (total de jogos e modo)
            # Aqui pegamos o count de IDs únicos da nossa tabela temporária
            unique_games = con.sql("SELECT count(distinct game_id) FROM tmp_pbp_transformed").fetchone()[0]
            
            con.execute(f"""
                INSERT INTO dev.bronze.ingestion_params (ingestion_id, param_name, param_value)
                VALUES 
                    ('{current_batch_id}', 'total_games', '{unique_games}'),
                    ('{current_batch_id}', 'execution_mode', 'duckdb_native_append')
            """)

            context.add_output_metadata({
                "eventos": check,
                "games": unique_games
            })

            context.log.info(f"Sucesso: {check} eventos de {unique_games} jogos apendados na Silver.")

