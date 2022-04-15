INSERT {{ BIGQUERY_DATASET }}.{{ RAW_DATA_TABLE }}
SELECT
    COALESCE(visit_id, 'NA') AS visit_id,
    event_time as event_time,
    COALESCE(user_id, -1) AS user_id,

    COALESCE(page_previous, 'NA') AS page_previous,
    COALESCE(page_current, 'NA') AS page_current,
    COALESCE(source_site, 'NA') AS source_site,
    COALESCE(source_api_version, 'NA') AS source_api_version,
    COALESCE(user_latitude, 'NA') AS user_latitude,

    COALESCE(user_longitude, 'NA') AS user_longitude,
    COALESCE(technical_browser, 'NA') AS technical_browser,
    COALESCE(technical_os, 'NA') AS technical_os,
    COALESCE(technical_lang, 'NA') AS technical_lang,
    COALESCE(device_type, 'NA') AS device_type,


    COALESCE(device_version, 'NA') AS device_version,
    COALESCE(technical_network, 'NA') AS technical_network,
    keep_private as keep_private

FROM {{ BIGQUERY_DATASET }}.{{ RAW_DATA_TABLE}}_{{ logical_date.strftime("%m%d%H") }} -- Creates a table name with month day and hour values appended to it
                                                                                            -- like listen_events_032313 for 23-03-2022 13:00:00