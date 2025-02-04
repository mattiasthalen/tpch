MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column bridge__loaded_at
  )
);

WITH line_items AS (
  SELECT
    *
  FROM silver.bag__tpch__line_items
  LEFT JOIN silver.int__uss_bridge__orders
    ON bag__tpch__line_items._hook__order = int__uss_bridge__orders._hook__order
    AND bag__tpch__line_items.line_item__valid_from < int__uss_bridge__orders.bridge__valid_to
    AND bag__tpch__line_items.line_item__valid_to > int__uss_bridge__orders.bridge__valid_from
  LEFT JOIN silver.int__uss_bridge__parts
    ON bag__tpch__line_items._hook__part = int__uss_bridge__parts._hook__part
    AND bag__tpch__line_items.line_item__valid_from < int__uss_bridge__parts.bridge__valid_to
    AND bag__tpch__line_items.line_item__valid_to > int__uss_bridge__parts.bridge__valid_from
  LEFT JOIN silver.int__uss_bridge__suppliers
    ON bag__tpch__line_items._hook__supplier = int__uss_bridge__suppliers._hook__supplier
    AND bag__tpch__line_items.line_item__valid_from < int__uss_bridge__suppliers.bridge__valid_to
    AND bag__tpch__line_items.line_item__valid_to > int__uss_bridge__suppliers.bridge__valid_from
), order__order_date AS (
  SELECT
    *,
    order__order_date AS event_date,
    1 AS measure__line_item_ordered,
    (
      line_item__receipt_date - order__order_date
    )::INT AS measure__line_item_delivery_lead_time
  FROM line_items
  INNER JOIN silver.bag__tpch__orders USING (_pit_hook__order)
  WHERE
    NOT order__order_date IS NULL
), line_item__commit_date AS (
  SELECT
    *,
    line_item__commit_date AS event_date,
    1 AS measure__line_item_committed,
    CASE WHEN line_item__receipt_date = line_item__commit_date THEN 1 END AS measure__line_item_delivered_on_time
  FROM line_items
  WHERE
    NOT line_item__commit_date IS NULL
), measures AS (
  SELECT
    *
  FROM order__order_date
  UNION ALL BY NAME
  SELECT
    *
  FROM line_item__commit_date
), final AS (
  SELECT
    'line_items' AS stage,
    _hook__line_item,
    _pit_hook__line_item,
    _pit_hook__order,
    _pit_hook__customer,
    _pit_hook__nation,
    _pit_hook__region,
    _pit_hook__part,
    _pit_hook__supplier,
    CONCAT('calendar|date|', event_date) AS _hook__calendar__date,
    MAX(measure__line_item_ordered) AS measure__line_item_ordered,
    MAX(measure__line_item_delivery_lead_time) AS measure__line_item_delivery_lead_time,
    MAX(measure__line_item_committed) AS measure__line_item_committed,
    MAX(measure__line_item_delivered_on_time) AS measure__line_item_delivered_on_time,
    GREATEST(
      line_item__loaded_at,
      bridge__loaded_at
    ) AS bridge__loaded_at,
    GREATEST(
      line_item__valid_from,
      bridge__valid_from
    ) AS bridge__valid_from,
    LEAST(
      line_item__valid_to,
      bridge__valid_to
    ) AS bridge__valid_to,
    bridge__valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS bridge__is_current
  FROM measures
  GROUP BY ALL
)
SELECT
  *
FROM final
WHERE
  bridge__loaded_at BETWEEN @start_ts AND @end_ts