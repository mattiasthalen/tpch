MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column part_supplier__loaded_at
  )
);

WITH source AS (
  SELECT
    *,
    'tpch' AS _sqlmesh__record_source
  FROM bronze.snp__tpch__part_suppliers
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY ps_partsuppkey ORDER BY _sqlmesh__valid_from) AS _sqlmesh__version,
    COALESCE(_sqlmesh__valid_to, '9999-12-31 23:59:59'::TIMESTAMP) AS _sqlmesh__valid_to,
    _sqlmesh__valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS _sqlmesh__is_current
  FROM source
), final AS (
  SELECT
    CONCAT(
      'part_supplier|',
      _sqlmesh__record_source,
      '|',
      ps_partsuppkey,
      '~valid_from|',
      _sqlmesh__valid_from
    )::BLOB AS _pit_hook__part_supplier,
    CONCAT('part_supplier|', _sqlmesh__record_source, '|', ps_partsuppkey)::BLOB AS _hook__part_supplier,
    CONCAT('part|', _sqlmesh__record_source, '|', ps_partkey)::BLOB AS _hook__part,
    CONCAT('supplier|', _sqlmesh__record_source, '|', ps_suppkey)::BLOB AS _hook__supplier,
    ps_availqty AS part_supplier__available_quantity,
    ps_supplycost AS part_supplier__supply_cost,
    ps_comment AS part_supplier__comment,
    _sqlmesh__hash_diff AS part_supplier__hash_diff,
    _sqlmesh__loaded_at AS part_supplier__loaded_at,
    _sqlmesh__version AS part_supplier__version,
    _sqlmesh__valid_from AS part_supplier__valid_from,
    _sqlmesh__valid_to AS part_supplier__valid_to,
    _sqlmesh__is_current AS part_supplier__is_current,
    _sqlmesh__record_source AS part_supplier__record_source
  FROM validity
)
SELECT
  *
FROM final
WHERE
  part_supplier__loaded_at BETWEEN @start_ts AND @end_ts