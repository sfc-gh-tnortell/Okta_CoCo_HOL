-- ============================================================
-- Step 5b: Create Cortex Search Service for Contracts
-- ============================================================
-- Enable semantic search over contract documents

USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE CORTEX SEARCH SERVICE PROD.FINAL.CONTRACT_SEARCH
    ON CONTRACT_TEXT
    ATTRIBUTES ACCOUNT_NAME, CONTRACT_NUMBER, PRODUCTS_LIST, TOTAL_VALUE, INDUSTRY
    WAREHOUSE = COMPUTE_WH
    TARGET_LAG = '1 day'
AS (
    SELECT 
        CONTRACT_ID,
        CONTRACT_NUMBER,
        ACCOUNT_ID,
        ACCOUNT_NAME,
        INDUSTRY,
        FILE_NAME,
        CONTRACT_TEXT,
        CONTRACT_SUMMARY,
        PRODUCTS_LIST,
        TOTAL_VALUE
    FROM PROD.FINAL.CONTRACT_CONTENT
);

-- Verify search service
SHOW CORTEX SEARCH SERVICES IN SCHEMA PROD.FINAL;
