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
  customer__nation__name,
  customer__nation__comment,
  customer__region__name,
  customer__region__comment,
  customer__loaded_at,
  customer__valid_from,
  customer__valid_to,
  customer__is_current
FROM silver.int__customers
WHERE
  customer__loaded_at BETWEEN @start_ts AND @end_ts