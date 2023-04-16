{{
  config({
    "materialized": "table",
    "persist_docs": {
      "relation": true
    }
  })
}}
-- First, we join the orders and shipment_deliveries tables using the order_id
-- column to get the order date and shipment date information for each order.
-- We also filter out any shipments that have already been delivered.
with order_shipment_info as (
    select "order_id", "order_date", "shipment_date"
    from {{ source('dim_order', 'orders') }}
    join {{ source('dim_d_create', 'shipments_deliveries') }} using ("order_id")
    where "delivery_date" is null
),

-- Next, we calculate the number of days between the order date and the shipment date.
-- We also calculate a flag to determine if the shipment is late or not.
shipment_timeliness as (
    select
        "order_id",
        "order_date",
        "shipment_date",
        "shipment_date" - "order_date" as days_to_ship,
        case when "shipment_date" >= "order_date" + 6 then true else false end as is_late_shipment
    from order_shipment_info
)

-- Finally, we aggregate the number of late shipments.
select count(*) as total_late_shipments
from shipment_timeliness
where is_late_shipment = true
