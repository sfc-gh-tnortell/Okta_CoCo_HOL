-- ============================================================
-- Step 5c: Create Contract Content Table with Enrichment
-- ============================================================
-- Enrich parsed PDF content with metadata from CRM tables

USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE PROD;

-- Create enriched content table joining PDF content with CRM data
CREATE OR REPLACE TABLE PROD.FINAL.CONTRACT_CONTENT AS
SELECT 
    cs.FILE_NAME,
    cs.CONTRACT_NUMBER,
    c.CONTRACT_ID,
    c.ACCOUNT_ID,
    a.ACCOUNT_NAME,
    a.INDUSTRY,
    cs.CONTENT AS CONTRACT_TEXT,
    -- Generate summary using Cortex LLM
    SNOWFLAKE.CORTEX.SUMMARIZE(cs.CONTENT) AS CONTRACT_SUMMARY,
    -- Get products list from subscriptions
    (
        SELECT LISTAGG(p.PRODUCT_NAME, ', ') WITHIN GROUP (ORDER BY p.PRODUCT_NAME)
        FROM PROD.RAW.SFDC_SUBSCRIPTION_CPQ s
        JOIN PROD.RAW.SFDC_PRODUCT p ON s.PRODUCT_ID = p.PRODUCT_ID
        WHERE s.CONTRACT_ID = c.CONTRACT_ID
    ) AS PRODUCTS_LIST,
    c.TCV AS TOTAL_VALUE,
    c.START_DATE,
    c.END_DATE,
    cs.CREATED_AT
FROM PROD.RAW.CONTRACT_SOURCE cs
JOIN PROD.RAW.SFDC_CONTRACT c ON cs.CONTRACT_NUMBER = c.CONTRACT_NUMBER
JOIN PROD.RAW.SFDC_ACCOUNT a ON c.ACCOUNT_ID = a.ACCOUNT_ID;

-- Verify content
SELECT 
    CONTRACT_NUMBER, 
    ACCOUNT_NAME, 
    PRODUCTS_LIST,
    LEFT(CONTRACT_SUMMARY, 200) AS SUMMARY_PREVIEW
FROM PROD.FINAL.CONTRACT_CONTENT 
LIMIT 5;
