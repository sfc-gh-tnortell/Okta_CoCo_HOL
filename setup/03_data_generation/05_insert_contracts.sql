-- ============================================================
-- Step 3e: Parse PDF Contracts and Insert into SFDC_CONTRACT
-- ============================================================
-- Parse PDF contracts and extract structured data into SFDC_CONTRACT table

USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE PROD;
USE SCHEMA RAW;

-- Create the source table by parsing PDFs from stage
CREATE OR REPLACE TABLE PROD.RAW.CONTRACT_SOURCE AS
SELECT 
    RELATIVE_PATH AS FILE_NAME,
    -- Extract contract number from filename (e.g., contract_CON-2026-000001.pdf)
    REPLACE(REPLACE(RELATIVE_PATH, 'contract_', ''), '.pdf', '') AS CONTRACT_NUMBER,
    -- Parse PDF content using AI_PARSE_DOCUMENT
    AI_PARSE_DOCUMENT(
        TO_FILE('@PROD.RAW.CONTRACTS_STAGE', RELATIVE_PATH),
        {'mode': 'LAYOUT'}
    ):content::VARCHAR AS CONTENT,
    CURRENT_TIMESTAMP() AS CREATED_AT
FROM DIRECTORY(@PROD.RAW.CONTRACTS_STAGE)
WHERE RELATIVE_PATH LIKE '%.pdf';

-- Verify extraction
SELECT CONTRACT_NUMBER, LEFT(CONTENT, 500) AS CONTENT_PREVIEW 
FROM PROD.RAW.CONTRACT_SOURCE 
LIMIT 5;

-- Insert contracts into SFDC_CONTRACT by joining parsed content with account data
INSERT INTO PROD.RAW.SFDC_CONTRACT
SELECT 
    'CON' || LPAD(ROW_NUMBER() OVER (ORDER BY a.ACCOUNT_ID)::VARCHAR, 6, '0') AS contract_id,
    cs.CONTRACT_NUMBER,
    a.ACCOUNT_ID,
    'USD' AS currency_iso_code,
    DATEADD(day, -MOD(ABS(HASH(a.ACCOUNT_ID)), 365), CURRENT_DATE()) AS start_date,
    DATEADD(month, 12, DATEADD(day, -MOD(ABS(HASH(a.ACCOUNT_ID)), 365), CURRENT_DATE())) AS end_date,
    12 AS contract_term,
    CASE WHEN a.ACCOUNT_STATUS = 'Active' THEN 'Activated' ELSE 'Expired' END AS contract_status,
    a.BILLING_STREET,
    a.BILLING_CITY,
    a.BILLING_STATE,
    a.BILLING_POSTALCODE,
    a.BILLING_COUNTRY,
    a.CARR_USD AS tcv,
    a.CARR_USD AS carr,
    ROUND(a.CARR_USD / 12, 2) AS mrr,
    a.CARR_USD AS arr,
    a.CREATED_DATE,
    DATEADD(day, MOD(ABS(HASH(a.ACCOUNT_ID || 'act')), 14) + 1, a.CREATED_DATE) AS activated_date,
    CASE WHEN MOD(ABS(HASH(a.ACCOUNT_ID || 'renew')), 100) < 70 THEN TRUE ELSE FALSE END AS auto_renew,
    a.CREATED_DATE::DATE AS customer_signed_date,
    CASE MOD(ABS(HASH(a.ACCOUNT_ID || 'title')), 7)
        WHEN 0 THEN 'CEO'
        WHEN 1 THEN 'CTO'
        WHEN 2 THEN 'CFO'
        WHEN 3 THEN 'VP IT'
        WHEN 4 THEN 'Director'
        WHEN 5 THEN 'IT Manager'
        ELSE 'CISO'
    END AS customer_signed_title
FROM PROD.RAW.CONTRACT_SOURCE cs
JOIN PROD.RAW.SFDC_ACCOUNT a 
    ON cs.CONTRACT_NUMBER = 'CON-' || YEAR(CURRENT_DATE()) || '-' || LPAD(
        (SELECT COUNT(*) + 1 FROM PROD.RAW.SFDC_ACCOUNT a2 WHERE a2.ACCOUNT_ID <= a.ACCOUNT_ID)::VARCHAR, 
        6, '0'
    );

-- Verify contracts created
SELECT COUNT(*) AS contract_count FROM PROD.RAW.SFDC_CONTRACT;
