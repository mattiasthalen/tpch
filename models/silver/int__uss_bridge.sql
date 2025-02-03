MODEL (
  kind VIEW
);

SELECT
  *
FROM silver.int__uss_bridge__customers
UNION ALL BY NAME
SELECT
  *
FROM silver.int__uss_bridge__nations
UNION ALL BY NAME
SELECT
  *
FROM silver.int__uss_bridge__order_lines
UNION ALL BY NAME
SELECT
  *
FROM silver.int__uss_bridge__orders
UNION ALL BY NAME
SELECT
  *
FROM silver.int__uss_bridge__part_suppliers
UNION ALL BY NAME
SELECT
  *
FROM silver.int__uss_bridge__parts
UNION ALL BY NAME
SELECT
  *
FROM silver.int__uss_bridge__regions
UNION ALL BY NAME
SELECT
  *
FROM silver.int__uss_bridge__suppliers