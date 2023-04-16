-- transform/public_holiday_orders.sql

{{config(
  materialized='table',
  schema='FRANEGAH6821_ANALYTICS'
)}}

WITH orders_shipment_dates AS (
  SELECT
    o."order_id",
    o."customer_id",
    o."product_id",
    o."unit_price",
    o."quantity",
    o."amount",
    d.year_num,
    d.month_of_the_year_num,
    sd."shipment_date",
    sd."delivery_date"
  FROM {{ source('dim_order', 'orders') }} o
  JOIN {{ source('dim_d_create', 'shipments_deliveries') }} sd ON o."order_id" = sd."order_id"
  JOIN {{ source('dim_d', 'dim_date') }} d ON sd."delivery_date" = d.calender_dt
),

public_holidays AS (
  SELECT
    d.year_num,
    month_of_the_year_num,
    day_of_the_month_num
  FROM {{ source('dim_d', 'dim_date') }} d
  WHERE day_of_the_week_num BETWEEN 1 AND 5
    AND working_day = 0
),

orders_on_public_holidays AS (
  SELECT
    orders_shipment_dates.year_num,
    orders_shipment_dates.month_of_the_year_num,
    COUNT(DISTINCT orders_shipment_dates."order_id") AS public_holiday_orders
  FROM orders_shipment_dates 
  JOIN public_holidays ON orders_shipment_dates.year_num = public_holidays.year_num
    AND orders_shipment_dates.month_of_the_year_num = public_holidays.month_of_the_year_num
    AND orders_shipment_dates."delivery_date" = DATE(public_holidays.year_num || '-' || public_holidays.month_of_the_year_num || '-' || public_holidays.day_of_the_month_num)
  WHERE orders_shipment_dates."delivery_date" BETWEEN DATEADD('month', -12, CURRENT_DATE()) AND CURRENT_DATE()
  GROUP BY 1, 2
)

SELECT
  CURRENT_DATE() AS ingestion_date,
  orders_on_public_holidays.public_holiday_orders AS tt_order_hol_jan,
  orders_on_public_holidays.public_holiday_orders AS tt_order_hol_feb,
  orders_on_public_holidays.public_holiday_orders AS tt_order_hol_march,
  orders_on_public_holidays.public_holiday_orders AS tt_order_hol_april,
  orders_on_public_holidays.public_holiday_orders AS tt_order_hol_may,
  orders_on_public_holidays.public_holiday_orders AS tt_order_hol_june,
  orders_on_public_holidays.public_holiday_orders AS tt_order_hol_july,
  orders_on_public_holidays.public_holiday_orders AS tt_order_hol_aug,
  orders_on_public_holidays.public_holiday_orders AS tt_order_hol_sept,
  orders_on_public_holidays.public_holiday_orders AS tt_order_hol_oct,
  orders_on_public_holidays.public_holiday_orders AS tt_order_hol_nov,
  orders_on_public_holidays.public_holiday_orders AS tt_order_hol_dec
FROM orders_on_public_holidays
WHERE orders_on_public_holidays.year_num = YEAR(CURRENT_DATE())
  AND orders_on_public_holidays.month_of_the_year_num = MONTH(CURRENT_DATE())
