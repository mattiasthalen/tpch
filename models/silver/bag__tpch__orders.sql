MODEL (
  kind VIEW
);

@DEF(primary_key := o_orderkey)

WITH source AS (
  SELECT
  * EXCLUDE(_sqlmesh__valid_from, _sqlmesh__valid_to),
    'tpch' AS _sqlmesh__record_source
  FROM bronze.snp__tpch__orders
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