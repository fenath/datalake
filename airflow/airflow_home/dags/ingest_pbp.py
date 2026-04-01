from airflow.sdk import DAG, task, dag
from db import get_connection, duck_conn
from datetime import datetime, timedelta
import uuid

@dag(
        description="Transforma os responses em registros de play_by_play",
        schedule=timedelta(hours=12),
        start_date=datetime(2026, 3, 26),
        )
def ingest_play_by_play():

    @task
    @duck_conn
    def responses_to_pbp(con):
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
            print(f"Sem eventos para ingestar.")
            return

        con.execute("""
                    BEGIN TRANSACTION;

                    INSERT INTO dev.silver.nhl_play_by_play SELECT * FROM tmp_pbp_transformed

                    -- # Registro do Batch para Auditoria
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

                    COMMIT;
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

        return {
            "message": f"Sucesso: {check} eventos de {unique_games} jogos apendados na Silver.",
            "eventos": check,
            "games": unique_games
        }
    responses_to_pbp()

ingest_play_by_play()
