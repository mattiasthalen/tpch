MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column bridge__loaded_at
  )
);

WITH nations AS (
  SELECT
    bag__tpch__nations._hook__nation,
    bag__tpch__nations._pit_hook__nation,
    int__uss_bridge__regions._pit_hook__region,
    GREATEST(bag__tpch__nations.nation__loaded_at, int__uss_bridge__regions.bridge__loaded_at) AS bridge__loaded_at,
    GREATEST(bag__tpch__nations.nation__valid_from, int__uss_bridge__regions.bridge__valid_from) AS bridge__valid_from,
    LEAST(bag__tpch__nations.nation__valid_to, int__uss_bridge__regions.bridge__valid_to) AS bridge__valid_to,
    bridge__valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS bridge__is_current
  FROM silver.bag__tpch__nations
  LEFT JOIN silver.int__uss_bridge__regions
    ON bag__tpch__nations._hook__region = int__uss_bridge__regions._hook__region
    AND bag__tpch__nations.nation__valid_from < int__uss_bridge__regions.bridge__valid_to
    AND bag__tpch__nations.nation__valid_to > int__uss_bridge__regions.bridge__valid_from
)
SELECT
  'nations' AS stage,
  *
FROM nations
WHERE
  bridge__loaded_at BETWEEN @start_ts AND @end_ts