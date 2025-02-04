MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column supplier__loaded_at
  )
);

WITH source AS (
  SELECT
    *,
    'tpch' AS _sqlmesh__record_source
  FROM bronze.snp__tpch__suppliers
), validity AS (
  SELECT
  * EXCLUDE(_sqlmesh__valid_to),
    ROW_NUMBER() OVER (PARTITION BY s_suppkey ORDER BY _sqlmesh__valid_from) AS _sqlmesh__version,
    COALESCE(_sqlmesh__valid_to, '9999-12-31 23:59:59'::TIMESTAMP) AS _sqlmesh__valid_to,
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
WHERE
  supplier__loaded_at BETWEEN @start_ts AND @end_ts