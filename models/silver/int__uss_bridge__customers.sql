MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column bridge__loaded_at
  )
);

WITH customers AS (
  SELECT
    _hook__customer,
    _pit_hook__customer,
    customer__loaded_at AS bridge__loaded_at,
    customer__valid_from AS bridge__valid_from,
    customer__valid_to AS bridge__valid_to,
    bridge__valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS bridge__is_current
  FROM silver.int__customers
)
SELECT
  'customers' AS stage,
  *
FROM customers
WHERE
  bridge__loaded_at BETWEEN @start_ts AND @end_ts