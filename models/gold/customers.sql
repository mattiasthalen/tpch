MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column customer__loaded_at
  )
);

SELECT
  _pit_hook__customer,
  customer__name,
  customer__address,
  customer__phone,
  customer__acctount_balance,
  customer__market_segment,
  customer__comment,
  customer__hash_diff,
  customer__loaded_at,
  customer__version,
  customer__valid_from,
  customer__valid_to,
  customer__is_current,
  customer__record_source
FROM silver.bag__tpch__customers
WHERE
  customer__loaded_at BETWEEN @start_ts AND @end_ts