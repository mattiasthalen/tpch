DROP TABLE IF EXISTS main.customer;
DROP TABLE IF EXISTS main.lineitem;
DROP TABLE IF EXISTS main.nation;
DROP TABLE IF EXISTS main.orders;
DROP TABLE IF EXISTS main.part;
DROP TABLE IF EXISTS main.partsupp;
DROP TABLE IF EXISTS main.region;
DROP TABLE IF EXISTS main.supplier;

CALL dbgen(sf = 1);