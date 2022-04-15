{{ config(materialized = 'table') }}
SELECT 
*
FROM
    (
        SELECT 
            ROW_NUMBER() OVER(partition by visit_id,user_id,technical_browser order by 1,2,3 ) as techkey,
            technical_browser as technical_browser,
            technical_os as technical_os,
            technical_lang as technical_lang,
            technical_network as technical_network,
            keep_private as keep_private
        FROM {{ source('staging', 'raw_data') }}

        UNION ALL

        SELECT 
            -1,
            'NA',
            'NA',
            'NA',
            'NA',
            null
 
    )
    group by 1,2,3,4,5,6