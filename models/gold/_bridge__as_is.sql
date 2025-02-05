MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column bridge__loaded_at
  )
);

SELECT
  stage,
  _hook__calendar__date,
  _pit_hook__customer,
  _pit_hook__line_item,
  _pit_hook__nation,
  _pit_hook__order,
  _pit_hook__part,
  _pit_hook__part_supplier,
  _pit_hook__region,
  _pit_hook__supplier,
  measure__line_item_ordered,
  measure__line_item_delivery_lead_time,
  measure__line_item_committed,
  measure__line_item_delivered_on_time,
  bridge__loaded_at,
  bridge__valid_from,
  bridge__valid_to,
  bridge__is_current
FROM silver.int__uss_bridge
WHERE
  1 = 1
  AND bridge__is_current = true
  AND bridge__loaded_at BETWEEN @start_ts AND @end_ts