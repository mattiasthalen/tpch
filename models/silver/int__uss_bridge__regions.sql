MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column bridge__loaded_at
  )
);

WITH regions AS (
  SELECT
    _hook__region,
    _pit_hook__region,
    region__loaded_at AS bridge__loaded_at,
    region__valid_from AS bridge__valid_from,
    region__valid_to AS bridge__valid_to,
    bridge__valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS bridge__is_current
  FROM silver.bag__tpch__regions
)
SELECT
  'regions' AS stage,
  *
FROM regions
WHERE
  bridge__loaded_at BETWEEN @start_ts AND @end_ts