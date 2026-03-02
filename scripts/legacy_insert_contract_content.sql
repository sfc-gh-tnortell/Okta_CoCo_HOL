-- ============================================================
-- LEGACY: Generate Contract Content Table Data
-- ============================================================
-- This script creates the CONTRACT_CONTENT table and populates it
-- with data directly from the SFDC tables. Use this if you need
-- to recreate the contract search content without the PDF files.
--
-- NOTE: The current workflow uses uploaded PDF files parsed via
-- setup/05_pdf_contracts/02_create_source_table.sql instead.
-- ============================================================

USE ROLE SYSADMIN;
USE WAREHOUSE DEFAULT_WH;
USE DATABASE PROD;
USE SCHEMA FINAL;

-- Create the CONTRACT_CONTENT table (used by Cortex Search)
CREATE OR REPLACE TABLE PROD.FINAL.CONTRACT_CONTENT (
    CONTRACT_ID VARCHAR(18) PRIMARY KEY,
    CONTRACT_NUMBER VARCHAR(30),
    ACCOUNT_ID VARCHAR(18),
    ACCOUNT_NAME VARCHAR(255),
    ACCOUNT_INDUSTRY VARCHAR(100),
    CONTRACT_START_DATE DATE,
    CONTRACT_END_DATE DATE,
    CONTRACT_VALUE NUMBER(18,2),
    ARR NUMBER(18,2),
    MRR NUMBER(18,2),
    AUTO_RENEW BOOLEAN,
    PRODUCTS_INCLUDED VARCHAR(4000),
    FULL_CONTENT VARCHAR(16777216),
    FILE_NAME VARCHAR(255),
    CREATED_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Populate CONTRACT_CONTENT from SFDC tables
INSERT INTO PROD.FINAL.CONTRACT_CONTENT
WITH contract_products AS (
    SELECT 
        s.CONTRACT_ID,
        LISTAGG(DISTINCT p.PRODUCT_NAME, ', ') WITHIN GROUP (ORDER BY p.PRODUCT_NAME) AS products_list,
        SUM(s.ARR) AS total_arr,
        SUM(s.MRR) AS total_mrr
    FROM PROD.RAW.SFDC_SUBSCRIPTION_CPQ s
    JOIN PROD.RAW.SFDC_PRODUCT p ON s.PRODUCT_ID = p.PRODUCT_ID
    GROUP BY s.CONTRACT_ID
)
SELECT
    c.CONTRACT_ID,
    c.CONTRACT_NUMBER,
    c.ACCOUNT_ID,
    a.ACCOUNT_NAME,
    a.INDUSTRY,
    c.START_DATE,
    c.END_DATE,
    c.TCV,
    COALESCE(cp.total_arr, c.ARR),
    COALESCE(cp.total_mrr, c.MRR),
    c.AUTO_RENEW,
    cp.products_list,
    -- Generate full searchable content
    'SOFTWARE LICENSE AND SERVICES AGREEMENT\n' ||
    '========================================\n\n' ||
    'Contract Number: ' || c.CONTRACT_NUMBER || '\n' ||
    'Account: ' || a.ACCOUNT_NAME || '\n' ||
    'Industry: ' || COALESCE(a.INDUSTRY, 'N/A') || '\n\n' ||
    'CUSTOMER INFORMATION\n' ||
    '--------------------\n' ||
    'Company: ' || a.ACCOUNT_NAME || '\n' ||
    'Address: ' || COALESCE(a.BILLING_STREET, '') || ', ' || 
    COALESCE(a.BILLING_CITY, '') || ', ' || 
    COALESCE(a.BILLING_STATE, '') || ' ' || 
    COALESCE(a.BILLING_POSTALCODE, '') || '\n' ||
    'Industry: ' || COALESCE(a.INDUSTRY, 'N/A') || '\n' ||
    'Sub-Industry: ' || COALESCE(a.SUB_INDUSTRY, 'N/A') || '\n\n' ||
    'CONTRACT TERMS\n' ||
    '--------------\n' ||
    'Effective Date: ' || c.START_DATE::VARCHAR || '\n' ||
    'Expiration Date: ' || c.END_DATE::VARCHAR || '\n' ||
    'Term Length: ' || c.CONTRACT_TERM::VARCHAR || ' months\n' ||
    'Auto-Renewal: ' || CASE WHEN c.AUTO_RENEW THEN 'Enabled' ELSE 'Disabled' END || '\n' ||
    'Signed By: ' || COALESCE(c.CUSTOMER_SIGNED_TITLE, 'Executive') || '\n' ||
    'Signed Date: ' || COALESCE(c.CUSTOMER_SIGNED_DATE::VARCHAR, 'N/A') || '\n\n' ||
    'LICENSED PRODUCTS\n' ||
    '-----------------\n' ||
    COALESCE(cp.products_list, 'See subscription details') || '\n\n' ||
    'PRICING SUMMARY\n' ||
    '---------------\n' ||
    'Total Contract Value: $' || TO_VARCHAR(c.TCV, '999,999,999.99') || '\n' ||
    'Annual Recurring Revenue: $' || TO_VARCHAR(COALESCE(cp.total_arr, c.ARR), '999,999,999.99') || '\n' ||
    'Monthly Recurring Revenue: $' || TO_VARCHAR(COALESCE(cp.total_mrr, c.MRR), '999,999,999.99') || '\n\n' ||
    'TERMS AND CONDITIONS\n' ||
    '--------------------\n' ||
    'This Agreement is entered into between Okta, Inc. ("Provider") and ' || a.ACCOUNT_NAME || ' ("Customer").\n\n' ||
    '1. LICENSE GRANT: Provider grants Customer a non-exclusive, non-transferable license to use the Services.\n' ||
    '2. PAYMENT TERMS: Customer agrees to pay all fees as specified in the pricing summary above.\n' ||
    '3. DATA PROTECTION: Provider shall maintain appropriate security measures to protect Customer data.\n' ||
    '4. CONFIDENTIALITY: Both parties agree to maintain confidentiality of proprietary information.\n' ||
    '5. LIMITATION OF LIABILITY: Provider liability shall not exceed fees paid in the prior 12 months.\n' ||
    '6. TERMINATION: Either party may terminate with 30 days written notice for material breach.\n\n' ||
    '[END OF CONTRACT DOCUMENT]' AS FULL_CONTENT,
    'contract_' || c.CONTRACT_NUMBER || '.pdf' AS FILE_NAME,
    CURRENT_TIMESTAMP() AS CREATED_DATE
FROM PROD.RAW.SFDC_CONTRACT c
JOIN PROD.RAW.SFDC_ACCOUNT a ON c.ACCOUNT_ID = a.ACCOUNT_ID
LEFT JOIN contract_products cp ON c.CONTRACT_ID = cp.CONTRACT_ID;

-- Verify data
SELECT COUNT(*) AS contract_count FROM PROD.FINAL.CONTRACT_CONTENT;
SELECT ACCOUNT_NAME, CONTRACT_NUMBER, PRODUCTS_INCLUDED 
FROM PROD.FINAL.CONTRACT_CONTENT 
LIMIT 5;

-- Create the Cortex Search Service on this table
CREATE OR REPLACE CORTEX SEARCH SERVICE PROD.FINAL.CONTRACT_SEARCH
    ON FULL_CONTENT
    ATTRIBUTES CONTRACT_NUMBER, ACCOUNT_NAME, ACCOUNT_INDUSTRY, PRODUCTS_INCLUDED
    WAREHOUSE = DEFAULT_WH
    TARGET_LAG = '1 hour'
    AS (
        SELECT 
            CONTRACT_NUMBER,
            ACCOUNT_NAME,
            ACCOUNT_INDUSTRY,
            PRODUCTS_INCLUDED,
            FULL_CONTENT
        FROM PROD.FINAL.CONTRACT_CONTENT
    );
