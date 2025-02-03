MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column region__loaded_at
  )
);

SELECT
  _pit_hook__region,
  region__name,
  region__comment,
  region__hash_diff,
  region__loaded_at,
  region__version,
  region__valid_from,
  region__valid_to,
  region__is_current,
  region__record_source
FROM silver.bag__tpch__regions
WHERE
  region__loaded_at BETWEEN @start_ts AND @end_ts