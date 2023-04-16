{{
  config({
    "materialized": "table",
    "schema": "FRANEGAH6821_ANALYTICS",
    "persist_docs": {
      "relation": true
    }
  })
}}

with cte as (select current_date as ingestion_date, 
        total_undelivered_shipments AS tt_undelivered_shipments
        FROM {{ source('dim_undeli_ship', 'total_undelivered_shipment') }}
),
cta as (select current_date as ingestion_date, 
        total_late_shipments AS tt_late_shipments
        FROM {{ source('dim_late_ship', 'late_shipmet') }}
)

select cte.ingestion_date, tt_late_shipments, tt_undelivered_shipments FROM cte
JOIN cta ON cte.ingestion_date = cta.ingestion_date
