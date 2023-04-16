{{
  config({
    "materialized": "table",
    "persist_docs": {
      "relation": true
    }
  })
}}

-- Extract the distinct dates from the shipment and delivery dates
WITH dates AS (
  SELECT DISTINCT "shipment_date" AS date
  FROM {{ source('dim_d_create', 'shipments_deliveries') }}
  UNION
  SELECT DISTINCT "delivery_date" AS date
  FROM {{ source('dim_d_create', 'shipments_deliveries') }}
),

-- Convert the dates to year, month, day, and day of the week
date_parts AS (
  SELECT 
    DATE_PART('year', date) AS year_num,
    DATE_PART('month', date) AS month_of_the_year_num,
    DATE_PART('day', date) AS day_of_the_month_num,
    DATE_PART('dow', date) AS day_of_the_week_num,
    date AS calender_dt
  FROM dates
),

-- Identify public holidays as days with day_of_the_week number in the range 1 - 5
public_holidays AS (
  SELECT 
    calender_dt,
    year_num,
    month_of_the_year_num,
    day_of_the_month_num,
    day_of_the_week_num,
    false AS working_day
  FROM date_parts
  WHERE day_of_the_week_num BETWEEN 1 AND 5
),

-- Create a table with all the dates and their respective details
dim_date AS (
  SELECT 
    calender_dt,
    year_num,
    month_of_the_year_num,
    day_of_the_month_num,
    day_of_the_week_num,
    true AS working_day
  FROM date_parts
  UNION
  SELECT *
  FROM public_holidays
)

-- Return the final table
SELECT *
FROM dim_date
