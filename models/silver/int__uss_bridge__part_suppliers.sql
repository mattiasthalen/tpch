MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column bridge__loaded_at
  )
);

WITH part_suppliers AS (
  SELECT
    bag__tpch__part_suppliers._pit_hook__part_supplier,
    bag__tpch__suppliers._pit_hook__supplier,
    bag__tpch__nations._pit_hook__nation,
    bag__tpch__regions._pit_hook__region,
    GREATEST(
      bag__tpch__part_suppliers.part_supplier__loaded_at,
      bag__tpch__suppliers.supplier__loaded_at,
      bag__tpch__nations.nation__loaded_at,
      bag__tpch__regions.region__loaded_at
    ) AS bridge__loaded_at,
    GREATEST(
      bag__tpch__part_suppliers.part_supplier__valid_from,
      bag__tpch__suppliers.supplier__valid_from,
      bag__tpch__nations.nation__valid_from,
      bag__tpch__regions.region__valid_from
    ) AS bridge__valid_from,
    LEAST(
      bag__tpch__part_suppliers.part_supplier__valid_to,
      bag__tpch__suppliers.supplier__valid_to,
      bag__tpch__nations.nation__valid_to,
      bag__tpch__regions.region__valid_to
    ) AS bridge__valid_to,
    bridge__valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS bridge__is_current
  FROM silver.bag__tpch__part_suppliers
  LEFT JOIN silver.bag__tpch__suppliers
    ON bag__tpch__part_suppliers._hook__supplier = bag__tpch__suppliers._hook__supplier
    AND bag__tpch__part_suppliers.part_supplier__valid_from < bag__tpch__suppliers.supplier__valid_to
    AND bag__tpch__part_suppliers.part_supplier__valid_to > bag__tpch__suppliers.supplier__valid_from
  LEFT JOIN silver.bag__tpch__nations
    ON bag__tpch__suppliers._hook__nation = bag__tpch__nations._hook__nation
    AND bag__tpch__part_suppliers.part_supplier__valid_from < bag__tpch__nations.nation__valid_to
    AND bag__tpch__part_suppliers.part_supplier__valid_to > bag__tpch__nations.nation__valid_from
  LEFT JOIN silver.bag__tpch__regions
    ON bag__tpch__nations._hook__region = bag__tpch__regions._hook__region
    AND bag__tpch__part_suppliers.part_supplier__valid_from < bag__tpch__regions.region__valid_to
    AND bag__tpch__part_suppliers.part_supplier__valid_to > bag__tpch__regions.region__valid_from
)
SELECT
  'part_suppliers' AS stage,
  *
FROM part_suppliers
WHERE
  bridge__loaded_at BETWEEN @start_ts AND @end_ts