{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT
    game_id,
    event_owner_team_id                                                    AS team_id,
    CAST(game_date AS DATE)                                                AS game_date,
    COUNT(*) FILTER (WHERE type_desc = 'faceoff')                         AS faceoffs_ganhos,
    COUNT(*) FILTER (WHERE type_desc IN ('shot-on-goal', 'goal'))         AS chutes_ao_gol,
    COUNT(*) FILTER (WHERE type_desc = 'goal')                            AS gols,
    COUNT(*) FILTER (WHERE type_desc = 'blocked-shot')                    AS bloqueado,
    COUNT(*) FILTER (WHERE type_desc = 'takeaway')                        AS desarmes,
    COUNT(*) FILTER (WHERE type_desc = 'giveaway')                        AS doacoes,
    COUNT(*) FILTER (WHERE type_desc = 'hit')                             AS rebatidas
FROM iceberg.silver.nhl_play_by_play
WHERE YEAR(game_date) = 2026
  AND event_owner_team_id IS NOT NULL
GROUP BY game_id, event_owner_team_id, game_date
ORDER BY game_id, event_owner_team_id
