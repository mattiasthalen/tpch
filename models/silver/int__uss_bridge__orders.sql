MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column bridge__loaded_at
  ),
);

WITH orders AS (
  SELECT
    bag__tpch__orders._pit_hook__order,
    bag__tpch__customers._pit_hook__customer,
    bag__tpch__nations._pit_hook__nation,
    bag__tpch__regions._pit_hook__region,
    GREATEST(
      bag__tpch__orders.order__loaded_at,
      bag__tpch__customers.customer__loaded_at,
      bag__tpch__nations.nation__loaded_at,
      bag__tpch__regions.region__loaded_at
    ) AS bridge__loaded_at,
    GREATEST(
      bag__tpch__orders.order__valid_from,
      bag__tpch__customers.customer__valid_from,
      bag__tpch__nations.nation__valid_from,
      bag__tpch__regions.region__valid_from
    ) AS bridge__valid_from,
    LEAST(
      bag__tpch__orders.order__valid_to,
      bag__tpch__customers.customer__valid_to,
      bag__tpch__nations.nation__valid_to,
      bag__tpch__regions.region__valid_to
    ) AS bridge__valid_to
  FROM silver.bag__tpch__orders
  LEFT JOIN silver.bag__tpch__customers
    ON bag__tpch__orders._hook__customer = bag__tpch__customers._hook__customer
    AND bag__tpch__orders.order__valid_from < bag__tpch__customers.customer__valid_to
    AND bag__tpch__orders.order__valid_to > bag__tpch__customers.customer__valid_from
  LEFT JOIN silver.bag__tpch__nations
    ON bag__tpch__customers._hook__nation = bag__tpch__nations._hook__nation
    AND bag__tpch__orders.order__valid_from < bag__tpch__nations.nation__valid_to
    AND bag__tpch__orders.order__valid_to > bag__tpch__nations.nation__valid_from
  LEFT JOIN silver.bag__tpch__regions
    ON bag__tpch__nations._hook__region = bag__tpch__regions._hook__region
    AND bag__tpch__orders.order__valid_from < bag__tpch__regions.region__valid_to
    AND bag__tpch__orders.order__valid_to > bag__tpch__regions.region__valid_from
)
SELECT
  'orders' AS stage,
  *
FROM orders
WHERE
  bridge__loaded_at BETWEEN @start_ts AND @end_ts