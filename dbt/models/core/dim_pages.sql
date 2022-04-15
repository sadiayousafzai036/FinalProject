
{{ config(materialized = 'table') }}
   SELECT 
                user_id as pagekey,
                page_previous as page_previous,
                page_current as page_current,
                source_site as source_site,
                source_api_version as source_api_version
            FROM {{ source('staging', 'raw_data') }}
    UNION ALL
            SELECT 
            -1,
            'NA',
            'NA',
            'NA',
            'NA'