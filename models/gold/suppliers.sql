MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column supplier__loaded_at
  )
);

SELECT
  _pit_hook__supplier,
  supplier__name,
  supplier__address,
  supplier__phone,
  supplier__acctount_balance,
  supplier__comment,
  supplier__nation__name,
  supplier__nation__comment,
  supplier__region__name,
  supplier__region__comment,
  supplier__loaded_at,
  supplier__valid_from,
  supplier__valid_to,
  supplier__is_current
FROM silver.int__suppliers
WHERE
  supplier__loaded_at BETWEEN @start_ts AND @end_ts