MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column bridge__loaded_at
  )
);

WITH line_items AS (
  SELECT
    *
  FROM silver.bag__tpch__line_items
  LEFT JOIN silver.bag__tpch__orders
    ON bag__tpch__line_items._hook__order = bag__tpch__orders._hook__order
    AND bag__tpch__line_items.line_item__valid_from < bag__tpch__orders.order__valid_to
    AND bag__tpch__line_items.line_item__valid_to > bag__tpch__orders.order__valid_from
  LEFT JOIN silver.bag__tpch__customers
    ON bag__tpch__orders._hook__customer = bag__tpch__customers._hook__customer
    AND bag__tpch__line_items.line_item__valid_from < bag__tpch__customers.customer__valid_to
    AND bag__tpch__line_items.line_item__valid_to > bag__tpch__customers.customer__valid_from
  LEFT JOIN silver.bag__tpch__nations
    ON bag__tpch__customers._hook__nation = bag__tpch__nations._hook__nation
    AND bag__tpch__line_items.line_item__valid_from < bag__tpch__nations.nation__valid_to
    AND bag__tpch__line_items.line_item__valid_to > bag__tpch__nations.nation__valid_from
  LEFT JOIN silver.bag__tpch__regions
    ON bag__tpch__nations._hook__region = bag__tpch__regions._hook__region
    AND bag__tpch__line_items.line_item__valid_from < bag__tpch__regions.region__valid_to
    AND bag__tpch__line_items.line_item__valid_to > bag__tpch__regions.region__valid_from
  LEFT JOIN silver.bag__tpch__parts
    ON bag__tpch__line_items._hook__part = bag__tpch__parts._hook__part
    AND bag__tpch__line_items.line_item__valid_from < bag__tpch__parts.part__valid_to
    AND bag__tpch__line_items.line_item__valid_to > bag__tpch__parts.part__valid_from
  LEFT JOIN silver.bag__tpch__suppliers
    ON bag__tpch__line_items._hook__supplier = bag__tpch__suppliers._hook__supplier
    AND bag__tpch__line_items.line_item__valid_from < bag__tpch__suppliers.supplier__valid_to
    AND bag__tpch__line_items.line_item__valid_to > bag__tpch__suppliers.supplier__valid_from
), order__order_date AS (
  SELECT
    *,
    order__order_date AS event_date,
    1 AS measure__line_item_ordered,
    (
      line_item__receipt_date - order__order_date
    )::INT AS measure__line_item_delivery_lead_time
  FROM line_items
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
    CONCAT('calendar|date|', event_date) AS _hook__calendar__date,
    _pit_hook__line_item,
    _pit_hook__order,
    _pit_hook__customer,
    _pit_hook__nation,
    _pit_hook__region,
    _pit_hook__part,
    _pit_hook__supplier,
    measure__line_item_ordered,
    measure__line_item_delivery_lead_time,
    measure__line_item_committed,
    measure__line_item_delivered_on_time,
    GREATEST(
      line_item__loaded_at,
      order__loaded_at,
      customer__loaded_at,
      nation__loaded_at,
      region__loaded_at,
      part__loaded_at,
      supplier__loaded_at
    ) AS bridge__loaded_at,
    GREATEST(
      line_item__valid_from,
      order__valid_from,
      customer__valid_from,
      nation__valid_from,
      region__valid_from,
      part__valid_from,
      supplier__valid_from
    ) AS bridge__valid_from,
    LEAST(
      line_item__valid_to,
      order__valid_to,
      customer__valid_to,
      nation__valid_to,
      region__valid_to,
      part__valid_to,
      supplier__valid_to
    ) AS bridge__valid_to,
    bridge__valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS bridge__is_current
  FROM measures
)
SELECT
  *
FROM final
WHERE
  bridge__loaded_at BETWEEN @start_ts AND @end_ts