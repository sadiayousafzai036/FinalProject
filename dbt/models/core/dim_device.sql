{{ config(materialized = 'table') }}
SELECT * FROM (
        SELECT 
            ROW_NUMBER() OVER(partition by visit_id,user_id,device_type order by 1,2,3 ) as devicekey,
            device_type as device_type,
            device_version as device_version
          
        FROM {{ source('staging', 'raw_data') }}
        
        UNION ALL

        SELECT 
            -1,
            'NA',
            'NA'
)
group by 1,2,3
 
    