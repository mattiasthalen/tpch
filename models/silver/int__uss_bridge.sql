MODEL (
  kind VIEW
);

SELECT
  * EXCLUDE(_hook__customer)
FROM silver.int__uss_bridge__customers
UNION ALL BY NAME
SELECT
  * EXCLUDE(_hook__nation)
FROM silver.int__uss_bridge__nations
UNION ALL BY NAME
SELECT
  * EXCLUDE(_hook__line_item)
FROM silver.int__uss_bridge__line_items
UNION ALL BY NAME
SELECT
  * EXCLUDE(_hook__order)
FROM silver.int__uss_bridge__orders
UNION ALL BY NAME
SELECT
  * EXCLUDE(_hook__part_supplier)
FROM silver.int__uss_bridge__part_suppliers
UNION ALL BY NAME
SELECT
  * EXCLUDE(_hook__part)
FROM silver.int__uss_bridge__parts
UNION ALL BY NAME
SELECT
  * EXCLUDE(_hook__region)
FROM silver.int__uss_bridge__regions
UNION ALL BY NAME
SELECT
  * EXCLUDE(_hook__supplier)
FROM silver.int__uss_bridge__suppliers