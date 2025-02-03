MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column order_line__loaded_at
  )
);

SELECT
  _pit_hook__order_line,
  order_line__line_number,
  order_line__quantity,
  order_line__extended_price,
  order_line__discount,
  order_line__tax,
  order_line__return_flag,
  order_line__line_status,
  order_line__ship_date,
  order_line__commit_date,
  order_line__receipt_date,
  order_line__shipping_instructions,
  order_line__ship_mode,
  order_line__comment,
  order_line__hash_diff,
  order_line__loaded_at,
  order_line__version,
  order_line__valid_from,
  order_line__valid_to,
  order_line__is_current,
  order_line__record_source
FROM silver.bag__tpch__order_lines
WHERE
  order_line__loaded_at BETWEEN @start_ts AND @end_ts