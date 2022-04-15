{{ config(
  materialized = 'table'
  ) }}

SELECT 

    dim_device.devicekey AS devicekey,
    dim_pages.pagekey AS pagekey,
    dim_technical.techkey AS techkey ,
    dim_datetime.dateKey AS dateKey,
    dim_loc.locationKey AS locationKey,
    raw_data.event_time AS timestamp,

    dim_device.device_type AS device_type,
    dim_device.device_version AS device_version,


    dim_pages.page_previous AS page_previous,
    dim_pages.page_current AS page_current,
    dim_pages.source_site AS source_site,
    dim_pages.source_api_version AS source_api_version,

    dim_loc.city AS city,
    dim_loc.stateName AS state,
    dim_loc.latitude AS latitude,
    dim_loc.longitude AS longitude,

    dim_datetime.date AS dateHour,
    dim_datetime.dayOfMonth AS dayOfMonth,
    dim_datetime.dayOfWeek AS dayOfWeek,
    
    dim_technical.technical_browser AS technical_browser,
    dim_technical.technical_os AS technical_os,
    dim_technical.technical_lang AS technical_lang,
    dim_technical.technical_network AS technical_network,
    dim_technical.keep_private AS keep_private
 FROM {{ source('staging', 'raw_data') }}
  LEFT JOIN {{ ref('dim_device') }} 
    ON raw_data.device_type = dim_device.device_type AND raw_data.device_version = dim_device.device_version
  LEFT JOIN {{ ref('dim_pages') }} 
    ON raw_data.page_previous= dim_pages.page_previous and raw_data.page_current = dim_pages.page_current and raw_data.source_site=dim_pages.source_site and raw_data.source_api_version=dim_pages.source_api_version
  LEFT JOIN {{ ref('dim_technical') }} 
    ON raw_data.technical_browser = dim_technical.technical_browser and raw_data.technical_os = dim_technical.technical_os and raw_data.technical_lang = dim_technical.technical_lang and raw_data.technical_network=dim_technical.technical_network
  LEFT JOIN {{ ref('dim_loc') }} 
    ON trunc(cast(raw_data.user_latitude as FLOAT64) ,2) = trunc(dim_loc.latitude,2) AND  trunc(cast(raw_data.user_longitude as FLOAT64) ,2) = trunc(dim_loc.longitude,2)
  LEFT JOIN {{ ref('dim_datetime') }} 
    ON dim_datetime.date = date_trunc(raw_data.event_time, HOUR)