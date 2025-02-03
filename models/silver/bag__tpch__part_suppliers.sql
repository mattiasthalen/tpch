MODEL (
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key _pit_hook__part_supplier
  )
);

@DEF(primary_key, "ps_partsuppkey");

WITH source AS (
  SELECT
    CONCAT(ps_partkey, '|', ps_suppkey)::BLOB AS ps_partsuppkey,
    *,
    'tpch' AS _sqlmesh__record_source
  FROM bronze.snp__tpch__part_suppliers
), affected_keys AS (
  SELECT DISTINCT
    @primary_key
  FROM source
  WHERE
    1 = 1 AND _sqlmesh__loaded_at BETWEEN @start_ts AND @end_ts
), affected_rows AS (
  SELECT
    source.*
  FROM source
  WHERE
    1 = 1
    AND EXISTS(
      SELECT
        *
      FROM affected_keys
      WHERE
        affected_keys.@primary_key = source.@primary_key
    )
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY @primary_key ORDER BY _sqlmesh__loaded_at) AS _sqlmesh__version,
    CASE
      WHEN _sqlmesh__version = 1
      THEN '0001-01-01 00:00:00'::TIMESTAMP
      ELSE _sqlmesh__loaded_at
    END AS _sqlmesh__valid_from,
    COALESCE(
      LEAD(_sqlmesh__loaded_at) OVER (PARTITION BY @primary_key ORDER BY _sqlmesh__loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS _sqlmesh__valid_to,
    _sqlmesh__valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS _sqlmesh__is_current
  FROM affected_rows
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