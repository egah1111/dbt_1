{{
    config(
        materialized='table',
        schema='FRANEGAH6821_ANALYTICS'
    )
}}

WITH public_holidays AS (
    SELECT
        "order_id",
        year_num,
        month_of_the_year_num,
        COUNT("order_id") AS num_orders
    FROM
        {{ source('dim_order', 'orders') }}
        JOIN {{ source('dim_d_create', 'shipments_deliveries') }} sd using ("order_id")
        JOIN {{ source('dim_d', 'dim_date') }} d ON sd."delivery_date" = d.calender_dt
    WHERE
        day_of_the_week_num BETWEEN 1 AND 5
        AND working_day = false
    GROUP BY
        "order_id",
        year_num,
        month_of_the_year_num
),

agg_public_holiday AS (
    SELECT
        CURRENT_DATE() AS ingestion_date,
        COALESCE(SUM(CASE WHEN public_holidays.year_num = DATE_PART('year', CURRENT_DATE()) AND public_holidays.month_of_the_year_num = 1 THEN public_holidays.num_orders END), 0) AS tt_order_hol_jan,
        COALESCE(SUM(CASE WHEN public_holidays.year_num = DATE_PART('year', CURRENT_DATE()) AND public_holidays.month_of_the_year_num = 2 THEN public_holidays.num_orders END), 0) AS tt_order_hol_feb,
        COALESCE(SUM(CASE WHEN public_holidays.year_num = DATE_PART('year', CURRENT_DATE()) AND public_holidays.month_of_the_year_num = 3 THEN public_holidays.num_orders END), 0) AS tt_order_hol_mar,
        COALESCE(SUM(CASE WHEN public_holidays.year_num = DATE_PART('year', CURRENT_DATE()) AND public_holidays.month_of_the_year_num = 4 THEN public_holidays.num_orders END), 0) AS tt_order_hol_apr,
        COALESCE(SUM(CASE WHEN public_holidays.year_num = DATE_PART('year', CURRENT_DATE()) AND public_holidays.month_of_the_year_num = 5 THEN public_holidays.num_orders END), 0) AS tt_order_hol_may,
        COALESCE(SUM(CASE WHEN public_holidays.year_num = DATE_PART('year', CURRENT_DATE()) AND public_holidays.month_of_the_year_num = 6 THEN public_holidays.num_orders END), 0) AS tt_order_hol_jun,
        COALESCE(SUM(CASE WHEN public_holidays.year_num = DATE_PART('year', CURRENT_DATE()) AND public_holidays.month_of_the_year_num = 6 THEN public_holidays.num_orders END), 0) AS tt_order_hol_july,
        COALESCE(SUM(CASE WHEN public_holidays.year_num = DATE_PART('year', CURRENT_DATE()) AND public_holidays.month_of_the_year_num = 6 THEN public_holidays.num_orders END), 0) AS tt_order_hol_aug,
        COALESCE(SUM(CASE WHEN public_holidays.year_num = DATE_PART('year', CURRENT_DATE()) AND public_holidays.month_of_the_year_num = 6 THEN public_holidays.num_orders END), 0) AS tt_order_hol_sept,
        COALESCE(SUM(CASE WHEN public_holidays.year_num = DATE_PART('year', CURRENT_DATE()) AND public_holidays.month_of_the_year_num = 6 THEN public_holidays.num_orders END), 0) AS tt_order_hol_oct,
        COALESCE(SUM(CASE WHEN public_holidays.year_num = DATE_PART('year', CURRENT_DATE()) AND public_holidays.month_of_the_year_num = 6 THEN public_holidays.num_orders END), 0) AS tt_order_hol_nov,
        COALESCE(SUM(CASE WHEN public_holidays.year_num = DATE_PART('year', CURRENT_DATE()) AND public_holidays.month_of_the_year_num = 6 THEN public_holidays.num_orders END), 0) AS tt_order_hol_dec
    FROM
        public_holidays
)

SELECT * FROM agg_public_holiday
