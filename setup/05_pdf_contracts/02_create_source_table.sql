-- ============================================================
-- Step 5b: Create Contract Source Table from PDFs
-- ============================================================
-- Parse PDF contracts and extract content into a source table
-- Similar approach to Gong transcripts

USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE PROD;
USE SCHEMA RAW;

-- Create the source table by parsing PDFs from stage
CREATE OR REPLACE TABLE PROD.RAW.CONTRACT_SOURCE AS
SELECT 
    METADATA$FILENAME AS FILE_NAME,
    -- Extract contract number from filename (e.g., contract_CON-2026-000001.pdf)
    REPLACE(REPLACE(METADATA$FILENAME, 'contract_', ''), '.pdf', '') AS CONTRACT_NUMBER,
    -- Parse PDF content using AI_PARSE_DOCUMENT
    SNOWFLAKE.CORTEX.AI_PARSE_DOCUMENT(
        '@PROD.RAW.CONTRACTS_STAGE',
        METADATA$FILENAME,
        {'mode': 'LAYOUT'}
    ):content::VARCHAR AS CONTENT,
    CURRENT_TIMESTAMP() AS CREATED_AT
FROM DIRECTORY(@PROD.RAW.CONTRACTS_STAGE)
WHERE METADATA$FILENAME LIKE '%.pdf';

-- Verify extraction
SELECT CONTRACT_NUMBER, LEFT(CONTENT, 500) AS CONTENT_PREVIEW 
FROM PROD.RAW.CONTRACT_SOURCE 
LIMIT 5;
