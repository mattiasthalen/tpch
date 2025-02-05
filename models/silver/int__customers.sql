MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column customer__loaded_at
  )
);

WITH customers AS (
  SELECT
    bag__tpch__customers._hook__customer,
    bag__tpch__customers.customer__name,
    bag__tpch__customers.customer__address,
    bag__tpch__customers.customer__phone,
    bag__tpch__customers.customer__acctount_balance,
    bag__tpch__customers.customer__market_segment,
    bag__tpch__customers.customer__comment,
    bag__tpch__nations.nation__name AS customer__nation__name,
    bag__tpch__nations.nation__comment AS customer__nation__comment,
    bag__tpch__regions.region__name AS customer__region__name,
    bag__tpch__regions.region__comment AS customer__region__comment,
    GREATEST(
      bag__tpch__customers.customer__loaded_at,
      bag__tpch__nations.nation__loaded_at,
      bag__tpch__regions.region__loaded_at
    ) AS customer__loaded_at,
    GREATEST(
      bag__tpch__customers.customer__valid_from,
      bag__tpch__nations.nation__valid_from,
      bag__tpch__regions.region__valid_from
    ) AS customer__valid_from,
    LEAST(
      bag__tpch__customers.customer__valid_to,
      bag__tpch__nations.nation__valid_to,
      bag__tpch__regions.region__valid_to
    ) AS customer__valid_to,
    customer__valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS customer__is_current
  FROM silver.bag__tpch__customers
  LEFT JOIN silver.bag__tpch__nations
    ON bag__tpch__customers._hook__nation = bag__tpch__nations._hook__nation
    AND bag__tpch__customers.customer__valid_from < bag__tpch__nations.nation__valid_to
    AND bag__tpch__customers.customer__valid_to > bag__tpch__nations.nation__valid_from
  LEFT JOIN silver.bag__tpch__regions
    ON bag__tpch__nations._hook__region = bag__tpch__regions._hook__region
    AND bag__tpch__customers.customer__valid_from < bag__tpch__regions.region__valid_to
    AND bag__tpch__customers.customer__valid_to > bag__tpch__regions.region__valid_from
)
SELECT
  CONCAT(_hook__customer::TEXT, '~valid_from|', customer__valid_from)::BLOB AS _pit_hook__customer,
  *
FROM customers
WHERE
  customer__loaded_at BETWEEN @start_ts AND @end_ts