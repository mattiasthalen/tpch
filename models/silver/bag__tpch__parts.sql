MODEL (
  kind VIEW
);

@DEF(primary_key := p_partkey)

WITH source AS (
  SELECT
  * EXCLUDE(_sqlmesh__valid_from, _sqlmesh__valid_to),
    'tpch' AS _sqlmesh__record_source
  FROM bronze.snp__tpch__parts
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
    CONCAT('part|', _sqlmesh__record_source, '|', p_partkey, '~valid_from|', _sqlmesh__valid_from)::BLOB AS _pit_hook__part,
    CONCAT('part|', _sqlmesh__record_source, '|', p_partkey)::BLOB AS _hook__part,
    p_name AS part__name,
    p_mfgr AS part__manufacturer,
    p_brand AS part__brand,
    p_type AS part__type,
    p_size AS part__size,
    p_container AS part__container,
    p_retailprice AS part__retail_price,
    p_comment AS part__comment,
    _sqlmesh__hash_diff AS part__hash_diff,
    _sqlmesh__loaded_at AS part__loaded_at,
    _sqlmesh__version AS part__version,
    _sqlmesh__valid_from AS part__valid_from,
    _sqlmesh__valid_to AS part__valid_to,
    _sqlmesh__is_current AS part__is_current,
    _sqlmesh__record_source AS part__record_source
  FROM validity
)
SELECT
  *
FROM final