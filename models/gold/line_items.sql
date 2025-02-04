MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column line_item__loaded_at
  )
);

SELECT
  _pit_hook__line_item,
  line_item__line_number,
  line_item__quantity,
  line_item__extended_price,
  line_item__discount,
  line_item__tax,
  line_item__return_flag,
  line_item__line_status,
  line_item__ship_date,
  line_item__commit_date,
  line_item__receipt_date,
  line_item__shipping_instructions,
  line_item__ship_mode,
  line_item__comment,
  line_item__hash_diff,
  line_item__loaded_at,
  line_item__version,
  line_item__valid_from,
  line_item__valid_to,
  line_item__is_current,
  line_item__record_source
FROM silver.bag__tpch__line_items
WHERE
  line_item__loaded_at BETWEEN @start_ts AND @end_ts