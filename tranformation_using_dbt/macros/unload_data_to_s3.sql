{% macro unload_data_to_s3() %}

    {{ log("Unloading data", True) }}
    COPY INTO 'analytics_export/franegah6821'
    FROM {{ source('dim_agg_ship', 'agg_shipment') }}
    FILE_FORMAT = (TYPE=CSV COMPRESSION=NONE)
    SINGLE=TRUE
    {{ log("Unloaded data", True) }}

{% endmacro %}