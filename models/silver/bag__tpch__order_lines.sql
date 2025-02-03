MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column order_line__loaded_at
  )
);

WITH source AS (
  SELECT
    *,
    'tpch' AS _sqlmesh__record_source
  FROM bronze.snp__tpch__line_items
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY l_lineitemkey ORDER BY _sqlmesh__valid_from) AS _sqlmesh__version,
    COALESCE(_sqlmesh__valid_to, '9999-12-31 23:59:59'::TIMESTAMP) AS _sqlmesh__valid_to,
    _sqlmesh__valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS _sqlmesh__is_current
  FROM source
), final AS (
  SELECT
    CONCAT(
      'order_line|',
      _sqlmesh__record_source,
      '|',
      l_lineitemkey,
      '~valid_from|',
      _sqlmesh__valid_from
    )::BLOB AS _pit_hook__order_line,
    CONCAT('order_line|', _sqlmesh__record_source, '|', l_lineitemkey)::BLOB AS _hook__order_line,
    CONCAT('order|', _sqlmesh__record_source, '|', l_orderkey)::BLOB AS _hook__order,
    CONCAT('part|', _sqlmesh__record_source, '|', l_partkey)::BLOB AS _hook__part,
    CONCAT('supplier|', _sqlmesh__record_source, '|', l_suppkey)::BLOB AS _hook__supplier,
    l_linenumber AS order_line__line_number,
    l_quantity AS order_line__quantity,
    l_extendedprice AS order_line__extended_price,
    l_discount AS order_line__discount,
    l_tax AS order_line__tax,
    l_returnflag AS order_line__return_flag,
    l_linestatus AS order_line__line_status,
    l_shipdate AS order_line__ship_date,
    l_commitdate AS order_line__commit_date,
    l_receiptdate AS order_line__receipt_date,
    l_shipinstruct AS order_line__shipping_instructions,
    l_shipmode AS order_line__ship_mode,
    l_comment AS order_line__comment,
    _sqlmesh__hash_diff AS order_line__hash_diff,
    _sqlmesh__loaded_at AS order_line__loaded_at,
    _sqlmesh__version AS order_line__version,
    _sqlmesh__valid_from AS order_line__valid_from,
    _sqlmesh__valid_to AS order_line__valid_to,
    _sqlmesh__is_current AS order_line__is_current,
    _sqlmesh__record_source AS order_line__record_source
  FROM validity
)
SELECT
  *
FROM final
WHERE
  order_line__loaded_at BETWEEN @start_ts AND @end_ts