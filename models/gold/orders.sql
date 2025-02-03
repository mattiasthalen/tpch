MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column order__loaded_at
  )
);

SELECT
  _pit_hook__order,
  order__order_status,
  order__total_price,
  order__order_date,
  order__order_priority,
  order__clerk,
  order__shipping_priority,
  order__comment,
  order__hash_diff,
  order__loaded_at,
  order__version,
  order__valid_from,
  order__valid_to,
  order__is_current,
  order__record_source
FROM silver.bag__tpch__orders
WHERE
  order__loaded_at BETWEEN @start_ts AND @end_ts