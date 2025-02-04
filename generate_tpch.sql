CREATE SCHEMA IF NOT EXISTS bronze;

DROP TABLE IF EXISTS raw__tpch__customers;
DROP TABLE IF EXISTS raw__tpch__line_items;
DROP TABLE IF EXISTS raw__tpch__nations;
DROP TABLE IF EXISTS raw__tpch__orders;
DROP TABLE IF EXISTS raw__tpch__parts;
DROP TABLE IF EXISTS raw__tpch__part_suppliers;
DROP TABLE IF EXISTS raw__tpch__regions;
DROP TABLE IF EXISTS raw__tpch__suppliers;

CALL dbgen(sf = 1);

ALTER TABLE customer RENAME TO raw__tpch__customers;
ALTER TABLE lineitem RENAME TO raw__tpch__line_items;
ALTER TABLE nation RENAME TO raw__tpch__nations;
ALTER TABLE orders RENAME TO raw__tpch__orders;
ALTER TABLE part RENAME TO raw__tpch__parts;
ALTER TABLE partsupp RENAME TO raw__tpch__part_suppliers;
ALTER TABLE region RENAME TO raw__tpch__regions;
ALTER TABLE supplier RENAME TO raw__tpch__suppliers;