MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column bridge__loaded_at
  )
);

WITH part_suppliers AS (
  SELECT
    bag__tpch__part_suppliers._hook__part_supplier,
    bag__tpch__part_suppliers._pit_hook__part_supplier,
    int__uss_bridge__parts._pit_hook__part,
    int__uss_bridge__suppliers._pit_hook__supplier,
    int__uss_bridge__suppliers._pit_hook__nation,
    int__uss_bridge__suppliers._pit_hook__region,
    GREATEST(
      bag__tpch__part_suppliers.part_supplier__loaded_at,
      int__uss_bridge__parts.bridge__loaded_at,
      int__uss_bridge__suppliers.bridge__loaded_at
    ) AS bridge__loaded_at,
    GREATEST(
      bag__tpch__part_suppliers.part_supplier__valid_from,
      int__uss_bridge__parts.bridge__valid_from,
      int__uss_bridge__suppliers.bridge__valid_from
    ) AS bridge__valid_from,
    LEAST(
      bag__tpch__part_suppliers.part_supplier__valid_to,
      int__uss_bridge__parts.bridge__valid_to,
      int__uss_bridge__suppliers.bridge__valid_to
    ) AS bridge__valid_to,
    bridge__valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS bridge__is_current
  FROM silver.bag__tpch__part_suppliers
  LEFT JOIN silver.int__uss_bridge__parts
    ON bag__tpch__part_suppliers._hook__part = int__uss_bridge__parts._hook__part
    AND bag__tpch__part_suppliers.part_supplier__valid_from < int__uss_bridge__parts.bridge__valid_to
    AND bag__tpch__part_suppliers.part_supplier__valid_to > int__uss_bridge__parts.bridge__valid_from
  LEFT JOIN silver.int__uss_bridge__suppliers
    ON bag__tpch__part_suppliers._hook__supplier = int__uss_bridge__suppliers._hook__supplier
    AND bag__tpch__part_suppliers.part_supplier__valid_from < int__uss_bridge__suppliers.bridge__valid_to
    AND bag__tpch__part_suppliers.part_supplier__valid_to > int__uss_bridge__suppliers.bridge__valid_from
)
SELECT
  'part_suppliers' AS stage,
  *
FROM part_suppliers
WHERE
  bridge__loaded_at BETWEEN @start_ts AND @end_ts