MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column bridge__loaded_at
  )
);

WITH parts AS (
  SELECT
    _pit_hook__part,
    part__loaded_at AS bridge__loaded_at,
    part__valid_from AS bridge__valid_from,
    part__valid_to AS bridge__valid_to
  FROM silver.bag__tpch__parts
)
SELECT
  'parts' AS stage,
  *
FROM parts
WHERE
  bridge__loaded_at BETWEEN @start_ts AND @end_ts