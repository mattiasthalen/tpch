MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column bridge__loaded_at
  )
);

WITH orders AS (
  SELECT
    bag__tpch__orders._hook__order,
    bag__tpch__orders._pit_hook__order,
    int__uss_bridge__customers._pit_hook__customer,
    int__uss_bridge__customers._pit_hook__nation__customer,
    int__uss_bridge__customers._pit_hook__region__customer,
    GREATEST(
      bag__tpch__orders.order__loaded_at,
      int__uss_bridge__customers.bridge__loaded_at
    ) AS bridge__loaded_at,
    GREATEST(
      bag__tpch__orders.order__valid_from,
      int__uss_bridge__customers.bridge__valid_from
    ) AS bridge__valid_from,
    LEAST(
      bag__tpch__orders.order__valid_to,
      int__uss_bridge__customers.bridge__valid_to
    ) AS bridge__valid_to
  FROM silver.bag__tpch__orders
  LEFT JOIN silver.int__uss_bridge__customers
    ON bag__tpch__orders._hook__customer = int__uss_bridge__customers._hook__customer
    AND bag__tpch__orders.order__valid_from < int__uss_bridge__customers.bridge__valid_to
    AND bag__tpch__orders.order__valid_to > int__uss_bridge__customers.bridge__valid_from
)
SELECT
  'orders' AS stage,
  *,
  bridge__valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS bridge__is_current
FROM orders
WHERE
  bridge__loaded_at BETWEEN @start_ts AND @end_ts