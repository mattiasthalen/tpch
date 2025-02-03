MODEL (
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key _pit_hook__order
  )
);

@DEF(primary_key, "o_orderkey");

WITH source AS (
  SELECT
    *,
    'tpch' AS _sqlmesh__record_source
  FROM bronze.snp__tpch__orders
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
    CONCAT('order|', _sqlmesh__record_source, '|', o_orderkey, '~valid_from|', _sqlmesh__valid_from)::BLOB AS _pit_hook__order,
    CONCAT('order|', _sqlmesh__record_source, '|', o_orderkey)::BLOB AS _hook__order,
    CONCAT('customer|', _sqlmesh__record_source, '|', o_custkey)::BLOB AS _hook__customer,
    o_orderstatus AS order__order_status,
    o_totalprice AS order__total_price,
    o_orderdate AS order__order_date,
    o_orderpriority AS order__order_priority,
    o_clerk AS order__clerk,
    o_shippriority AS order__shipping_priority,
    o_comment AS order__comment,
    _sqlmesh__hash_diff AS order__hash_diff,
    _sqlmesh__loaded_at AS order__loaded_at,
    _sqlmesh__version AS order__version,
    _sqlmesh__valid_from AS order__valid_from,
    _sqlmesh__valid_to AS order__valid_to,
    _sqlmesh__is_current AS order__is_current,
    _sqlmesh__record_source AS order__record_source
  FROM validity
)
SELECT
  *
FROM final