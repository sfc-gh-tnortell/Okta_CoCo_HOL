-- ============================================================
-- Step 7d: Create Cortex Search Service for Transcripts
-- ============================================================
-- Enables semantic search over call transcripts

CREATE OR REPLACE CORTEX SEARCH SERVICE PROD.FINAL.TRANSCRIPT_SEARCH
    ON FULL_CONTENT
    ATTRIBUTES ACCOUNT_NAME, CALL_DATE, CALL_TYPE, SENTIMENT, KEY_INSIGHTS
    WAREHOUSE = COMPUTE_WH
    TARGET_LAG = '1 day'
AS (
    SELECT 
        TRANSCRIPT_ID,
        ACCOUNT_ID,
        ACCOUNT_NAME,
        CALL_DATE,
        CALL_TYPE,
        PARTICIPANTS,
        SUMMARY,
        KEY_INSIGHTS,
        SENTIMENT,
        FULL_CONTENT,
        FILE_NAME
    FROM PROD.FINAL.TRANSCRIPT_CONTENT
);

-- Verify search service
SHOW CORTEX SEARCH SERVICES IN SCHEMA PROD.FINAL;
