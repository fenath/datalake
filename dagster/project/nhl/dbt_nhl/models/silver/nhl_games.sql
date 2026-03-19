-- models/silver/nhl_games.sql

{{ config(
    materialized='table',
    schema='silver'
) }}

WITH raw AS (
    SELECT
        endpoint,
        requested_at,
        CAST(response AS JSON) AS response
    FROM iceberg.bronze.nhl_api_calls
    WHERE endpoint LIKE 'https://api-web.nhle.com/v1/schedule/%'
      AND status = 200
      AND NOT EXISTS (
          SELECT 1
          FROM iceberg.silver.nhl_games g
          WHERE g.endpoint = nhl_api_calls.endpoint
      )
),

weeks AS (
    SELECT
        r.endpoint,
        r.requested_at,
        week
    FROM raw r
    CROSS JOIN UNNEST(
        CAST(json_extract(r.response, '$.gameWeek') AS ARRAY(JSON))
    ) AS t(week)
),

games AS (
    SELECT
        w.endpoint,
        w.requested_at,
        w.week,
        game
    FROM weeks w
    CROSS JOIN UNNEST(
        CAST(json_extract(w.week, '$.games') AS ARRAY(JSON))
    ) AS t(game)
)

SELECT
    CAST(json_extract_scalar(game, '$.id')                                    AS BIGINT)    AS game_id,
    CAST(json_extract_scalar(week, '$.date')                                  AS DATE)      AS date,
    json_extract_scalar(game, '$.venue.default')                                            AS venue,
    CAST(json_extract_scalar(game, '$.startTimeUTC')                          AS TIMESTAMP WITH TIME ZONE) AS start_time_utc,
    json_extract_scalar(game, '$.gameState')                                                AS game_state,
    json_extract_scalar(game, '$.gameScheduleState')                                        AS game_schedule_state,
    CAST(json_extract_scalar(game, '$.awayTeam.id')                           AS INTEGER)   AS away_id,
    json_extract_scalar(game, '$.awayTeam.abbrev')                                          AS away_abbrev,
    json_extract_scalar(game, '$.awayTeam.commonName.default')                              AS away_common_name,
    CAST(json_extract_scalar(game, '$.homeTeam.id')                           AS INTEGER)   AS home_id,
    json_extract_scalar(game, '$.homeTeam.abbrev')                                          AS home_abbrev,
    json_extract_scalar(game, '$.homeTeam.commonName.default')                              AS home_common_name,
    CAST(json_extract_scalar(game, '$.gameType')                              AS INTEGER)   AS game_type,
    endpoint,
    CAST(
        regexp_extract(endpoint, '/schedule/(\d{4}-\d{2}-\d{2})', 1)         AS DATE)      AS endpoint_date,
    requested_at

FROM games
WHERE CAST(json_extract_scalar(game, '$.gameType') AS INTEGER) <= 3
