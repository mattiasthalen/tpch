MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column part__loaded_at
  )
);

SELECT
  _pit_hook__part,
  part__name,
  part__manufacturer,
  part__brand,
  part__type,
  part__size,
  part__container,
  part__retail_price,
  part__comment,
  part__hash_diff,
  part__loaded_at,
  part__version,
  part__valid_from,
  part__valid_to,
  part__is_current,
  part__record_source
FROM silver.bag__tpch__parts
WHERE
  part__loaded_at BETWEEN @start_ts AND @end_ts