MODEL (
  kind VIEW
);

SELECT
  stage,
  _hook__calendar__date,
  _pit_hook__customer,
  _pit_hook__line_item,
  _pit_hook__order,
  _pit_hook__part,
  _pit_hook__part_supplier,
  _pit_hook__supplier,
  measure__line_item_ordered,
  measure__line_item_delivery_lead_time,
  measure__line_item_committed,
  measure__line_item_delivered_on_time,
  bridge__loaded_at,
  bridge__valid_from,
  bridge__valid_to,
  bridge__is_current
FROM gold._bridge__as_of
WHERE
  1 = 1
  AND string_split(_hook__calendar__date, '|')[-1]::DATE >= bridge__valid_from
  AND string_split(_hook__calendar__date, '|')[-1]::DATE <= bridge__valid_to