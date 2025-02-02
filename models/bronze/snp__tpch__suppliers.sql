MODEL (
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key _sqlmesh__hash_diff
  )
);

SELECT
  *,
  @generate_surrogate_key(COLUMNS(*)) AS _sqlmesh__hash_diff,
  @execution_ts::TIMESTAMP AS _sqlmesh__loaded_at
FROM main.supplier