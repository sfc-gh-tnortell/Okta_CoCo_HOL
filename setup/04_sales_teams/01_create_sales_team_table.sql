-- ============================================================
-- Step 4a: Create Sales Team Table
-- ============================================================

CREATE OR REPLACE TABLE PROD.RAW.SALES_TEAM (
    TEAM_ID VARCHAR(10) NOT NULL PRIMARY KEY,
    TERRITORY VARCHAR(50),
    TIMEZONE VARCHAR(50),
    REGION VARCHAR(50),
    ACCOUNT_EXECUTIVE VARCHAR(100),
    SALES_ENGINEER VARCHAR(100),
    SDR VARCHAR(100),
    CREATED_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
