SELECT
    (SELECT COUNT(*) FROM {{ params.schema }}.FACT_FIRE_DETECTION WHERE BATCH_ID = '{{ ds }}')
      = (SELECT ROWS_CLEAN FROM {{ params.schema }}.RUN_SUMMARY WHERE BATCH_ID = '{{ ds }}')
      AS fact_count_matches_summary,

    (SELECT COUNT(*) FROM {{ params.schema }}.REJECTED_DETECTIONS WHERE BATCH_ID = '{{ ds }}')
      = (SELECT ROWS_REJECTED FROM {{ params.schema }}.RUN_SUMMARY WHERE BATCH_ID = '{{ ds }}')
      AS rejected_count_matches_summary,

    (SELECT COUNT(*) = COUNT(DISTINCT DETECTION_ID)
       FROM {{ params.schema }}.FACT_FIRE_DETECTION WHERE BATCH_ID = '{{ ds }}')
      AS no_duplicate_ids,

    (SELECT COUNT_IF(DETECTION_ID IS NULL OR DETECTED_AT_UTC IS NULL OR COUNTRY_ISO3 IS NULL
                     OR LATITUDE NOT BETWEEN -90 AND 90 OR LONGITUDE NOT BETWEEN -180 AND 180) = 0
       FROM {{ params.schema }}.FACT_FIRE_DETECTION WHERE BATCH_ID = '{{ ds }}')
      AS no_nulls_and_valid_coordinates;