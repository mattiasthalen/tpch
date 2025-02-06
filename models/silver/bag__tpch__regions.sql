MODEL (
  kind VIEW
);

@DEF(primary_key := r_regionkey)

WITH source AS (
  SELECT
  * EXCLUDE(_sqlmesh__valid_from, _sqlmesh__valid_to),
    'tpch' AS _sqlmesh__record_source
  FROM bronze.snp__tpch__regions
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