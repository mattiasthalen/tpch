MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column region__loaded_at
  )
);

WITH source AS (
  SELECT
    *,
    'tpch' AS _sqlmesh__record_source
  FROM bronze.snp__tpch__regions
), validity AS (
  SELECT
    *
    EXCLUDE (_sqlmesh__valid_to),
    ROW_NUMBER() OVER (PARTITION BY r_regionkey ORDER BY _sqlmesh__valid_from) AS _sqlmesh__version,
    COALESCE(_sqlmesh__valid_to, '9999-12-31 23:59:59'::TIMESTAMP) AS _sqlmesh__valid_to,
    _sqlmesh__valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS _sqlmesh__is_current
  FROM source
), final AS (
  SELECT
    CONCAT(
      'supplier|',
      _sqlmesh__record_source,
      '|',
      r_regionkey,
      '~valid_from|',
      _sqlmesh__valid_from
    )::BLOB AS _pit_hook__region,
    CONCAT('supplier|', _sqlmesh__record_source, '|', r_regionkey)::BLOB AS _hook__region,
    r_name AS region__name,
    r_comment AS region__comment,
    _sqlmesh__hash_diff AS region__hash_diff,
    _sqlmesh__loaded_at AS region__loaded_at,
    _sqlmesh__version AS region__version,
    _sqlmesh__valid_from AS region__valid_from,
    _sqlmesh__valid_to AS region__valid_to,
    _sqlmesh__is_current AS region__is_current,
    _sqlmesh__record_source AS region__record_source
  FROM validity
)
SELECT
  *
FROM final
WHERE
  region__loaded_at BETWEEN @start_ts AND @end_ts