MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column part_supplier__loaded_at
  )
);

SELECT
  _pit_hook__part_supplier,
  part_supplier__available_quantity,
  part_supplier__supply_cost,
  part_supplier__comment,
  part_supplier__hash_diff,
  part_supplier__loaded_at,
  part_supplier__version,
  part_supplier__valid_from,
  part_supplier__valid_to,
  part_supplier__is_current,
  part_supplier__record_source
FROM silver.bag__tpch__part_suppliers
WHERE
  part_supplier__loaded_at BETWEEN @start_ts AND @end_ts