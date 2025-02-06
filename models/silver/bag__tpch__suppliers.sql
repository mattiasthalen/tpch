MODEL (
  kind VIEW
);

@DEF(primary_key := s_suppkey)

WITH source AS (
  SELECT
  * EXCLUDE(_sqlmesh__valid_from, _sqlmesh__valid_to),
    'tpch' AS _sqlmesh__record_source
  FROM bronze.snp__tpch__suppliers
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
    CONCAT('supplier|', _sqlmesh__record_source, '|', s_suppkey, '~valid_from|', _sqlmesh__valid_from)::BLOB AS _pit_hook__supplier,
    CONCAT('supplier|', _sqlmesh__record_source, '|', s_suppkey)::BLOB AS _hook__supplier,
    CONCAT('nation|', _sqlmesh__record_source, '|', s_nationkey)::BLOB AS _hook__nation,
    s_name AS supplier__name,
    s_address AS supplier__address,
    s_phone AS supplier__phone,
    s_acctbal AS supplier__acctount_balance,
    s_comment AS supplier__comment,
    _sqlmesh__hash_diff AS supplier__hash_diff,
    _sqlmesh__loaded_at AS supplier__loaded_at,
    _sqlmesh__version AS supplier__version,
    _sqlmesh__valid_from AS supplier__valid_from,
    _sqlmesh__valid_to AS supplier__valid_to,
    _sqlmesh__is_current AS supplier__is_current,
    _sqlmesh__record_source AS supplier__record_source
  FROM validity
)
SELECT
  *
FROM final