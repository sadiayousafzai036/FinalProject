{{ config(materialized = 'table') }}

SELECT {{ dbt_utils.surrogate_key(['latitude', 'longitude', 'city', 'stateName']) }} as locationKey,
city,
stateCode,
stateName,
latitude,
longitude
FROM
    (
        SELECT 
            COALESCE(state.city, 'NA') as city,
            COALESCE(state.state_code, 'NA') as stateCode,
            COALESCE(state.state_name, 'NA') as stateName,
            CAST(user_latitude AS FLOAT64) as latitude,
            CAST(user_longitude AS FLOAT64) as longitude
        FROM {{ source('staging', 'raw_data') }}
        LEFT JOIN {{ ref('state') }} on 
        
        trunc(cast(raw_data.user_latitude as FLOAT64) ,2) = trunc(state.latitude,2) or
        trunc(cast(raw_data.user_longitude as FLOAT64),2) = trunc(state.longitude ,2)
        GROUP BY 1,2,3,4,5
        UNION ALL

        SELECT 
            'NA',
            'NA',
            'NA',
            0.0,
            0.0
    )
group by 1,2,3,4,5,6