MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column bridge__loaded_at
  ),
);

WITH order_lines AS (
  SELECT
    bag__tpch__order_lines._pit_hook__order_line,
    bag__tpch__orders._pit_hook__order,
    bag__tpch__customers._pit_hook__customer,
    bag__tpch__nations._pit_hook__nation,
    bag__tpch__regions._pit_hook__region,
    bag__tpch__parts._pit_hook__part,
    bag__tpch__suppliers._pit_hook__supplier,
    GREATEST(
      bag__tpch__order_lines.order_line__loaded_at,
      bag__tpch__orders.order__loaded_at,
      bag__tpch__customers.customer__loaded_at,
      bag__tpch__nations.nation__loaded_at,
      bag__tpch__regions.region__loaded_at,
      bag__tpch__parts.part__loaded_at,
      bag__tpch__suppliers.supplier__loaded_at
    ) AS bridge__loaded_at,
    GREATEST(
      bag__tpch__order_lines.order_line__valid_from,
      bag__tpch__orders.order__valid_from,
      bag__tpch__customers.customer__valid_from,
      bag__tpch__nations.nation__valid_from,
      bag__tpch__regions.region__valid_from,
      bag__tpch__parts.part__valid_from,
      bag__tpch__suppliers.supplier__valid_from
    ) AS bridge__valid_from,
    LEAST(
      bag__tpch__order_lines.order_line__valid_to,
      bag__tpch__orders.order__valid_to,
      bag__tpch__customers.customer__valid_to,
      bag__tpch__nations.nation__valid_to,
      bag__tpch__regions.region__valid_to,
      bag__tpch__parts.part__valid_to,
      bag__tpch__suppliers.supplier__valid_to
    ) AS bridge__valid_to
  FROM silver.bag__tpch__order_lines
  LEFT JOIN silver.bag__tpch__orders
    ON bag__tpch__order_lines._hook__order = bag__tpch__orders._hook__order
    AND bag__tpch__order_lines.order_line__valid_from < bag__tpch__orders.order__valid_to
    AND bag__tpch__order_lines.order_line__valid_to > bag__tpch__orders.order__valid_from
  LEFT JOIN silver.bag__tpch__customers
    ON bag__tpch__orders._hook__customer = bag__tpch__customers._hook__customer
    AND bag__tpch__order_lines.order_line__valid_from < bag__tpch__customers.customer__valid_to
    AND bag__tpch__order_lines.order_line__valid_to > bag__tpch__customers.customer__valid_from
  LEFT JOIN silver.bag__tpch__nations
    ON bag__tpch__customers._hook__nation = bag__tpch__nations._hook__nation
    AND bag__tpch__order_lines.order_line__valid_from < bag__tpch__nations.nation__valid_to
    AND bag__tpch__order_lines.order_line__valid_to > bag__tpch__nations.nation__valid_from
  LEFT JOIN silver.bag__tpch__regions
    ON bag__tpch__nations._hook__region = bag__tpch__regions._hook__region
    AND bag__tpch__order_lines.order_line__valid_from < bag__tpch__regions.region__valid_to
    AND bag__tpch__order_lines.order_line__valid_to > bag__tpch__regions.region__valid_from
  LEFT JOIN silver.bag__tpch__parts
    ON bag__tpch__order_lines._hook__part = bag__tpch__parts._hook__part
    AND bag__tpch__order_lines.order_line__valid_from < bag__tpch__parts.part__valid_to
    AND bag__tpch__order_lines.order_line__valid_to > bag__tpch__parts.part__valid_from
  LEFT JOIN silver.bag__tpch__suppliers
    ON bag__tpch__order_lines._hook__supplier = bag__tpch__suppliers._hook__supplier
    AND bag__tpch__order_lines.order_line__valid_from < bag__tpch__suppliers.supplier__valid_to
    AND bag__tpch__order_lines.order_line__valid_to > bag__tpch__suppliers.supplier__valid_from
)
SELECT
  'order_lines' AS stage,
  *
FROM order_lines
WHERE
  bridge__loaded_at BETWEEN @start_ts AND @end_ts