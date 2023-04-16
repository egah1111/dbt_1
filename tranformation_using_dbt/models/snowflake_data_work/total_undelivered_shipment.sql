-- dbt transformation to find total number of undelivered shipments
{{
  config(
    materialized='table',
    table='undelivered_shipments'
  )
}}

WITH cte_orders AS (
  SELECT "order_id", "order_date"
  FROM {{ source('dim_order', 'orders') }}
  WHERE DATEDIFF(day, "order_date", '2022-09-15') >= 15
),
cte_shipments AS (
  SELECT "order_id", "shipment_date", "delivery_date"
  FROM {{ source('dim_d_create', 'shipments_deliveries') }}
)
SELECT COUNT(*) AS total_undelivered_shipments
FROM cte_orders 
JOIN cte_shipments ON cte_orders."order_id" = cte_shipments."order_id"
WHERE "shipment_date" IS NULL AND "delivery_date" IS NULL
