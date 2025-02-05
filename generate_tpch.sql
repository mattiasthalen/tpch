
DROP TABLE IF EXISTS main.customer;
DROP TABLE IF EXISTS main.lineitem;
DROP TABLE IF EXISTS main.nation;
DROP TABLE IF EXISTS main.orders;
DROP TABLE IF EXISTS main.part;
DROP TABLE IF EXISTS main.partsupp;
DROP TABLE IF EXISTS main.region;
DROP TABLE IF EXISTS main.supplier;

CALL dbgen(sf = 1);

-- Simulate extract & load
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE OR REPLACE VIEW bronze.raw__tpch__customers AS SELECT * FROM main.customer;
CREATE OR REPLACE VIEW bronze.raw__tpch__line_items AS SELECT * FROM main.lineitem;
CREATE OR REPLACE VIEW bronze.raw__tpch__nations AS SELECT * FROM main.nation;
CREATE OR REPLACE VIEW bronze.raw__tpch__orders AS SELECT * FROM main.orders;
CREATE OR REPLACE VIEW bronze.raw__tpch__parts AS SELECT * FROM main.part;
CREATE OR REPLACE VIEW bronze.raw__tpch__part_suppliers AS SELECT * FROM main.partsupp;
CREATE OR REPLACE VIEW bronze.raw__tpch__regions AS SELECT * FROM main.region;
CREATE OR REPLACE VIEW bronze.raw__tpch__suppliers AS SELECT * FROM main.supplier;