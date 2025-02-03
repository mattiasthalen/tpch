MODEL (
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key _pit_hook__customer
  )
);

@DEF(primary_key, "c_custkey");

WITH source AS (
  SELECT
    *,
    'tpch' AS _sqlmesh__record_source
  FROM bronze.snp__tpch__customers
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