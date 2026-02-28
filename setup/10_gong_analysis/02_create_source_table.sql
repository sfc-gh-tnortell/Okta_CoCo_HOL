-- Step 2: Create Source Table for Cortex Search
-- Cortex Search requires a regular table (not a directory table)

-- Step 2a: Create file format for text files
CREATE OR REPLACE FILE FORMAT PROD.RAW.TEXT_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = NONE
    RECORD_DELIMITER = NONE;

-- Step 2b: Create the source table with parsed content
-- Filename format: AccountName_YYYY-MM-DD_call_N.txt (account name may contain underscores)
CREATE OR REPLACE TABLE PROD.RAW.GONG_TRANSCRIPT_SOURCE AS
SELECT 
    METADATA$FILENAME AS FILE_NAME,
    REPLACE(
        REPLACE(
            REGEXP_REPLACE(METADATA$FILENAME, '_\\d{4}-\\d{2}-\\d{2}_call_\\d+\\.txt$', ''),
            '_', ' '
        ),
        'and', '&'
    ) AS ACCOUNT_NAME,
    TRY_TO_DATE(
        REGEXP_SUBSTR(METADATA$FILENAME, '\\d{4}-\\d{2}-\\d{2}'),
        'YYYY-MM-DD'
    ) AS CALL_DATE,
    $1::VARCHAR AS CONTENT
FROM @PROD.RAW.GONG_TRANSCRIPTS_STAGE
    (FILE_FORMAT => 'PROD.RAW.TEXT_FORMAT', PATTERN => '.*\.txt');
