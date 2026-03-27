INSERT INTO dev.bronze.request_queue
WITH last_date AS (
     SELECT 
        regexp_extract(endpoint, '\d{4}-\d{2}-\d{2}')::DATE 
            AS dt
     FROM dev.bronze.nhl_api_calls
     WHERE endpoint LIKE '%schedule%' ORDER BY dt desc LIMIT 1
 ),
dates AS (
    SELECT CAST(dt + INTERVAL (i) DAY AS DATE) AS schedule_date
    FROM last_date,
      generate_series(1, (today() - dt)::INT) AS t(i)
    ORDER BY schedule_date
    ),
endpoints AS (
  SELECT 
      UUID() as request_id,
      ('https://api-web.nhle.com/v1/schedule/' || schedule_date) AS endpoint,
      'pending' AS status,
      NOW() AS created_at
  FROM dates d
)
SELECT * from endpoints d
WHERE NOT EXISTS (
    SELECT 1 FROM dev.bronze.request_queue q
    WHERE d.endpoint = q.endpoint
    AND q.status IN ('pending', 'done')
    );

