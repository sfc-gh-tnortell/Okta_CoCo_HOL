-- Step 4: Create Sentiment Table (Metadata Only)
-- Extracts sentiment from source table, storing only metadata (no content duplication)

CREATE OR REPLACE TABLE PROD.RAW.GONG_CALL_SENTIMENT AS
SELECT
    FILE_NAME,
    ACCOUNT_NAME,
    CALL_DATE,
    REPLACE(SPLIT_PART(FILE_NAME, '_call_', 2), '.txt', '')::INT AS CALL_NUMBER,
    AI_SENTIMENT(CONTENT):categories[0]:sentiment::VARCHAR AS SENTIMENT_CATEGORY
FROM PROD.RAW.GONG_TRANSCRIPT_SOURCE;
