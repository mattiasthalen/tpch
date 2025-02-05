MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column supplier__loaded_at
  )
);

WITH suppliers AS (
  SELECT
    bag__tpch__suppliers._pit_hook__supplier,
    bag__tpch__suppliers._hook__supplier,
    bag__tpch__suppliers._hook__nation,
    bag__tpch__suppliers.supplier__name,
    bag__tpch__suppliers.supplier__address,
    bag__tpch__suppliers.supplier__phone,
    bag__tpch__suppliers.supplier__acctount_balance,
    bag__tpch__suppliers.supplier__comment,
    bag__tpch__nations.nation__name AS supplier__nation__name,
    bag__tpch__nations.nation__comment AS supplier__nation__comment,
    bag__tpch__regions.region__name AS supplier__region__name,
    bag__tpch__regions.region__comment AS supplier__region__comment,
    GREATEST(
      bag__tpch__suppliers.supplier__loaded_at,
      bag__tpch__nations.nation__loaded_at,
      bag__tpch__regions.region__loaded_at
    ) AS supplier__loaded_at,
    GREATEST(
      bag__tpch__suppliers.supplier__valid_from,
      bag__tpch__nations.nation__valid_from,
      bag__tpch__regions.region__valid_from
    ) AS supplier__valid_from,
    LEAST(
      bag__tpch__suppliers.supplier__valid_to,
      bag__tpch__nations.nation__valid_to,
      bag__tpch__regions.region__valid_to
    ) AS supplier__valid_to,
    supplier__valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS supplier__is_current
  FROM silver.bag__tpch__suppliers
  LEFT JOIN silver.bag__tpch__nations
    ON bag__tpch__suppliers._hook__nation = bag__tpch__nations._hook__nation
    AND bag__tpch__suppliers.supplier__valid_from < bag__tpch__nations.nation__valid_to
    AND bag__tpch__suppliers.supplier__valid_to > bag__tpch__nations.nation__valid_from
  LEFT JOIN silver.bag__tpch__regions
    ON bag__tpch__nations._hook__region = bag__tpch__regions._hook__region
    AND bag__tpch__suppliers.supplier__valid_from < bag__tpch__regions.region__valid_to
    AND bag__tpch__suppliers.supplier__valid_to > bag__tpch__regions.region__valid_from
)
SELECT
  *
FROM suppliers
WHERE
  supplier__loaded_at BETWEEN @start_ts AND @end_ts