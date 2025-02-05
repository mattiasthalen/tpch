MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column bridge__loaded_at
  )
);

WITH suppliers AS (
  SELECT
    _hook__supplier,
    _pit_hook__supplier,
    supplier__loaded_at AS bridge__loaded_at,
    supplier__valid_from AS bridge__valid_from,
    supplier__valid_to AS bridge__valid_to,
    bridge__valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS bridge__is_current
  FROM silver.int__suppliers
)
SELECT
  'suppliers' AS stage,
  *
FROM suppliers
WHERE
  bridge__loaded_at BETWEEN @start_ts AND @end_ts