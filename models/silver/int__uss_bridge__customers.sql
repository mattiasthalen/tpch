MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column bridge__loaded_at
  )
);

WITH customers AS (
  SELECT
    bag__tpch__customers._hook__customer,
    bag__tpch__customers._pit_hook__customer,
    int__uss_bridge__nations._pit_hook__nation AS _pit_hook__nation__customer,
    int__uss_bridge__nations._pit_hook__region AS _pit_hook__region__customer,
    GREATEST(
      bag__tpch__customers.customer__loaded_at,
      int__uss_bridge__nations.bridge__loaded_at
    ) AS bridge__loaded_at,
    GREATEST(
      bag__tpch__customers.customer__valid_from,
      int__uss_bridge__nations.bridge__valid_from
    ) AS bridge__valid_from,
    LEAST(
      bag__tpch__customers.customer__valid_to,
      int__uss_bridge__nations.bridge__valid_to
    ) AS bridge__valid_to,
    bridge__valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS bridge__is_current
  FROM silver.bag__tpch__customers
  LEFT JOIN silver.int__uss_bridge__nations
    ON bag__tpch__customers._hook__nation = int__uss_bridge__nations._hook__nation
    AND bag__tpch__customers.customer__valid_from < int__uss_bridge__nations.bridge__valid_to
    AND bag__tpch__customers.customer__valid_to > int__uss_bridge__nations.bridge__valid_from
)
SELECT
  'customers' AS stage,
  *
FROM customers
WHERE
  bridge__loaded_at BETWEEN @start_ts AND @end_ts