MODEL (
  kind SCD_TYPE_2_BY_COLUMN (
    unique_key r_regionkey,
    valid_from_name _sqlmesh__valid_from,
    valid_to_name _sqlmesh__valid_to,
    columns [_sqlmesh__hash_diff],
    disable_restatement TRUE
  )
);

SELECT
  *,
  @generate_surrogate_key(COLUMNS(*)) AS _sqlmesh__hash_diff,
  @execution_ts::TIMESTAMP AS _sqlmesh__loaded_at
FROM main.raw__tpch__regions