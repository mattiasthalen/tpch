MODEL (
  kind VIEW
);

@DEF(primary_key := c_custkey)

WITH source AS (
  SELECT
    * EXCLUDE(_sqlmesh__valid_from, _sqlmesh__valid_to),
    'tpch' AS _sqlmesh__record_source
  FROM bronze.snp__tpch__customers
), validity AS (
  SELECT
    *
    ROW_NUMBER() OVER (PARTITION BY @primary_key ORDER BY _sqlmesh__loaded_at) AS _sqlmesh__version,
    _sqlmesh__loaded_at AS _sqlmesh__valid_from,
    COALESCE(
        LEAD(_sqlmesh__valid_to) OVER (PARTITION BY @primary_key, ORDER BY _sqlmesh__loaded_at),
    '9999-12-31 23:59:59'::TIMESTAMP) AS _sqlmesh__valid_to,
    _sqlmesh__valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS _sqlmesh__is_current
  FROM source
), final AS (
  SELECT
    CONCAT('customer|', _sqlmesh__record_source, '|', c_custkey, '~valid_from|', _sqlmesh__valid_from)::BLOB AS _pit_hook__customer,
    CONCAT('customer|', _sqlmesh__record_source, '|', c_custkey)::BLOB AS _hook__customer,
    CONCAT('nation|', _sqlmesh__record_source, '|', c_nationkey)::BLOB AS _hook__nation,
    c_name AS customer__name,
    c_address AS customer__address,
    c_phone AS customer__phone,
    c_acctbal AS customer__acctount_balance,
    c_mktsegment AS customer__market_segment,
    c_comment AS customer__comment,
    _sqlmesh__hash_diff AS customer__hash_diff,
    _sqlmesh__loaded_at AS customer__loaded_at,
    _sqlmesh__version AS customer__version,
    _sqlmesh__valid_from AS customer__valid_from,
    _sqlmesh__valid_to AS customer__valid_to,
    _sqlmesh__is_current AS customer__is_current,
    _sqlmesh__record_source AS customer__record_source
  FROM validity
)
SELECT
  *
FROM final