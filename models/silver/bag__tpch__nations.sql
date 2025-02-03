MODEL (
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key _pit_hook__nation
  )
);

@DEF(primary_key, "n_nationkey");

WITH source AS (
  SELECT
    *,
    'tpch' AS _sqlmesh__record_source
  FROM bronze.snp__tpch__nations
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