MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column nation__loaded_at
  )
);

SELECT
  _pit_hook__nation,
  nation__name,
  nation__comment,
  nation__hash_diff,
  nation__loaded_at,
  nation__version,
  nation__valid_from,
  nation__valid_to,
  nation__is_current,
  nation__record_source
FROM silver.bag__tpch__nations
WHERE
  nation__loaded_at BETWEEN @start_ts AND @end_ts