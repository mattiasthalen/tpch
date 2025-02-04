MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column bridge__loaded_at
  )
);

WITH suppliers AS (
  SELECT
    bag__tpch__suppliers._hook__supplier,
    bag__tpch__suppliers._pit_hook__supplier,
    int__uss_bridge__nations._pit_hook__nation,
    int__uss_bridge__nations._pit_hook__region,
    GREATEST(
      bag__tpch__suppliers.supplier__loaded_at,
      int__uss_bridge__nations.bridge__loaded_at
    ) AS bridge__loaded_at,
    GREATEST(
      bag__tpch__suppliers.supplier__valid_from,
      int__uss_bridge__nations.bridge__valid_from
    ) AS bridge__valid_from,
    LEAST(
      bag__tpch__suppliers.supplier__valid_to,
      int__uss_bridge__nations.bridge__valid_to
    ) AS bridge__valid_to,
    bridge__valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS bridge__is_current
  FROM silver.bag__tpch__suppliers
  LEFT JOIN silver.int__uss_bridge__nations
    ON bag__tpch__suppliers._hook__nation = int__uss_bridge__nations._hook__nation
    AND bag__tpch__suppliers.supplier__valid_from < int__uss_bridge__nations.bridge__valid_to
    AND bag__tpch__suppliers.supplier__valid_to > int__uss_bridge__nations.bridge__valid_from
)
SELECT
  'suppliers' AS stage,
  *
FROM suppliers
WHERE
  bridge__loaded_at BETWEEN @start_ts AND @end_ts