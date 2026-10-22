BEGIN TRANSACTION;

DELETE FROM {{ params.schema }}.{{ params.table }}
WHERE BATCH_ID = '{{ ds }}';

COPY INTO {{ params.schema }}.{{ params.table }}
FROM @{{ params.schema }}.{{ params.stage }}/tables/date={{ ds }}/
FILES = ('{{ params.table | lower }}.csv')
FILE_FORMAT = (FORMAT_NAME = '{{ params.schema }}.{{ params.file_format }}')
ON_ERROR = ABORT_STATEMENT
FORCE = TRUE;

COMMIT;