-- ============================================================
-- Step 5a: Create Stage for PDF Contracts
-- ============================================================

CREATE OR REPLACE STAGE PROD.RAW.CONTRACTS_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for storing PDF contract documents';

SHOW STAGES LIKE 'CONTRACTS_STAGE' IN SCHEMA PROD.RAW;
