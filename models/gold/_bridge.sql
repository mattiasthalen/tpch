MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column bridge__loaded_at
  )
);

SELECT
  *
FROM silver.int__uss_bridge
WHERE
  bridge__loaded_at BETWEEN @start_ts AND @end_ts