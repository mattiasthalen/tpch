MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column nation__loaded_at
  )
);

WITH source AS (
  SELECT
    *,
    'tpch' AS _sqlmesh__record_source
  FROM bronze.snp__tpch__nations
), validity AS (
  SELECT
    *
    EXCLUDE (_sqlmesh__valid_to),
    ROW_NUMBER() OVER (PARTITION BY n_nationkey ORDER BY _sqlmesh__valid_from) AS _sqlmesh__version,
    COALESCE(_sqlmesh__valid_to, '9999-12-31 23:59:59'::TIMESTAMP) AS _sqlmesh__valid_to,
    _sqlmesh__valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS _sqlmesh__is_current
  FROM source
), final AS (
  SELECT
    CONCAT('nation|', _sqlmesh__record_source, '|', n_nationkey, '~valid_from|', _sqlmesh__valid_from)::BLOB AS _pit_hook__nation,
    CONCAT('nation|', _sqlmesh__record_source, '|', n_nationkey)::BLOB AS _hook__nation,
    CONCAT('region|', _sqlmesh__record_source, '|', n_regionkey)::BLOB AS _hook__region,
    n_name AS nation__name,
    n_comment AS nation__comment,
    _sqlmesh__hash_diff AS nation__hash_diff,
    _sqlmesh__loaded_at AS nation__loaded_at,
    _sqlmesh__version AS nation__version,
    _sqlmesh__valid_from AS nation__valid_from,
    _sqlmesh__valid_to AS nation__valid_to,
    _sqlmesh__is_current AS nation__is_current,
    _sqlmesh__record_source AS nation__record_source
  FROM validity
)
SELECT
  *
FROM final
WHERE
  nation__loaded_at BETWEEN @start_ts AND @end_ts