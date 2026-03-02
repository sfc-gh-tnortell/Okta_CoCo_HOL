-- ============================================================
-- Step 3d: Create Stage for PDF Contracts
-- ============================================================

USE ROLE SYSADMIN;
USE WAREHOUSE DEFAULT_WH;
USE DATABASE PROD;
USE SCHEMA RAW;

-- Create stage for PDF contracts
CREATE OR REPLACE STAGE PROD.RAW.CONTRACTS_STAGE
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT = 'Stage for storing PDF contract documents';

-- Verify stage created
SHOW STAGES LIKE 'CONTRACTS_STAGE' IN SCHEMA PROD.RAW;

-- ============================================================
-- MANUAL STEP: Upload PDF contracts via Snowsight
-- ============================================================
-- 1. Navigate to Data → Databases → PROD → RAW → Stages
-- 2. Click on CONTRACTS_STAGE
-- 3. Click + Files → Select all PDF files from unstructured_data/contracts_pdf/
-- 4. Click Upload
-- ============================================================

-- Refresh directory after upload
ALTER STAGE PROD.RAW.CONTRACTS_STAGE REFRESH;

-- Verify files uploaded
SELECT * FROM DIRECTORY(@PROD.RAW.CONTRACTS_STAGE) LIMIT 10;
