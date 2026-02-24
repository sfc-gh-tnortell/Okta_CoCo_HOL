-- ============================================================
-- Step 2e: Create SFDC_OPPORTUNITY Table (NEW)
-- ============================================================
-- Tracks sales opportunities including failed expansion attempts

CREATE OR REPLACE TABLE PROD.RAW.SFDC_OPPORTUNITY (
    OPPORTUNITY_ID VARCHAR(18) NOT NULL PRIMARY KEY,
    OPPORTUNITY_NAME VARCHAR(255),
    ACCOUNT_ID VARCHAR(18),
    CONTRACT_ID VARCHAR(18),
    PRODUCT_ID VARCHAR(18),
    STAGE VARCHAR(40),
    STATUS VARCHAR(40),
    AMOUNT NUMBER(18,2),
    CLOSE_DATE DATE,
    CREATED_DATE TIMESTAMP_NTZ(9),
    LOSS_REASON VARCHAR(255),
    COMPETITOR VARCHAR(255),
    NEXT_STEPS VARCHAR(1000),
    DESCRIPTION VARCHAR(4000)
);
