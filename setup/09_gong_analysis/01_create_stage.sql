-- ============================================================
-- Step 11a: Create Stage and Upload Gong Transcripts
-- ============================================================

USE ROLE SYSADMIN;
USE WAREHOUSE DEFAULT_WH;
USE DATABASE PROD;
USE SCHEMA RAW;

-- Create stage for Gong transcripts
CREATE OR REPLACE STAGE PROD.RAW.GONG_TRANSCRIPTS_STAGE
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

-- Verify stage created
SHOW STAGES LIKE 'GONG_TRANSCRIPTS_STAGE' IN SCHEMA PROD.RAW;

-- ============================================================
-- MANUAL STEP: Upload transcript files via Snowsight
-- ============================================================
-- 1. Navigate to Data → Databases → PROD → RAW → Stages
-- 2. Click on GONG_TRANSCRIPTS_STAGE
-- 3. Click + Files → Select all .txt files from unstructured_data/gong_transcripts/
-- 4. Click Upload
-- ============================================================

-- Refresh directory after upload
ALTER STAGE PROD.RAW.GONG_TRANSCRIPTS_STAGE REFRESH;

-- Verify files uploaded
SELECT * FROM DIRECTORY(@PROD.RAW.GONG_TRANSCRIPTS_STAGE) LIMIT 10;
