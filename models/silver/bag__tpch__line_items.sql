MODEL (
  kind VIEW
);

@DEF(primary_key := ls_lineitemkey)

WITH source AS (
  SELECT
  CONCAT(l_orderkey, '|', l_linenumber) AS ls_lineitemkey,
  * EXCLUDE(_sqlmesh__valid_from, _sqlmesh__valid_to),
    'tpch' AS _sqlmesh__record_source
  FROM bronze.snp__tpch__line_items
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
    CONCAT(
      'line_item|',
      _sqlmesh__record_source,
      '|',
      l_lineitemkey,
      '~valid_from|',
      _sqlmesh__valid_from
    )::BLOB AS _pit_hook__line_item,
    CONCAT('line_item|', _sqlmesh__record_source, '|', l_lineitemkey)::BLOB AS _hook__line_item,
    CONCAT('order|', _sqlmesh__record_source, '|', l_orderkey)::BLOB AS _hook__order,
    CONCAT('part|', _sqlmesh__record_source, '|', l_partkey)::BLOB AS _hook__part,
    CONCAT('supplier|', _sqlmesh__record_source, '|', l_suppkey)::BLOB AS _hook__supplier,
    l_linenumber AS line_item__line_number,
    l_quantity AS line_item__quantity,
    l_extendedprice AS line_item__extended_price,
    l_discount AS line_item__discount,
    l_tax AS line_item__tax,
    l_returnflag AS line_item__return_flag,
    l_linestatus AS line_item__line_status,
    l_shipdate AS line_item__ship_date,
    l_commitdate AS line_item__commit_date,
    l_receiptdate AS line_item__receipt_date,
    l_shipinstruct AS line_item__shipping_instructions,
    l_shipmode AS line_item__ship_mode,
    l_comment AS line_item__comment,
    _sqlmesh__hash_diff AS line_item__hash_diff,
    _sqlmesh__loaded_at AS line_item__loaded_at,
    _sqlmesh__version AS line_item__version,
    _sqlmesh__valid_from AS line_item__valid_from,
    _sqlmesh__valid_to AS line_item__valid_to,
    _sqlmesh__is_current AS line_item__is_current,
    _sqlmesh__record_source AS line_item__record_source
  FROM validity
)
SELECT
  *
FROM final