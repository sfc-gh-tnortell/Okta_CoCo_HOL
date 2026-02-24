-- ============================================================
-- Step 7a: Create Contract Content Table
-- ============================================================
-- Stores searchable contract content (text representation)
-- Note: PARSE_DOCUMENT doesn't work with client-side encrypted stages

CREATE OR REPLACE TABLE PROD.FINAL.CONTRACT_CONTENT (
    CONTRACT_ID VARCHAR(18) PRIMARY KEY,
    CONTRACT_NUMBER VARCHAR(30),
    ACCOUNT_ID VARCHAR(18),
    ACCOUNT_NAME VARCHAR(255),
    FILE_NAME VARCHAR(255),
    CONTRACT_TEXT VARCHAR(16777216),
    CONTRACT_SUMMARY VARCHAR(4000),
    PRODUCTS_LIST VARCHAR(4000),
    TOTAL_VALUE NUMBER(18,2),
    CREATED_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Populate with contract data
INSERT INTO PROD.FINAL.CONTRACT_CONTENT
SELECT 
    c.CONTRACT_ID,
    c.CONTRACT_NUMBER,
    c.ACCOUNT_ID,
    a.ACCOUNT_NAME,
    'contract_' || c.CONTRACT_NUMBER || '.pdf' AS file_name,
    -- Create text representation of contract
    'SOFTWARE LICENSE AND SERVICES AGREEMENT

Contract Number: ' || c.CONTRACT_NUMBER || '
Effective Date: ' || c.START_DATE::VARCHAR || '
End Date: ' || c.END_DATE::VARCHAR || '

PARTIES:
This Agreement is entered into between SecureID Solutions ("Provider") and ' || a.ACCOUNT_NAME || ' ("Customer").

CUSTOMER INFORMATION:
Company: ' || a.ACCOUNT_NAME || '
Address: ' || a.BILLING_STREET || ', ' || a.BILLING_CITY || ', ' || a.BILLING_STATE || ' ' || a.BILLING_POSTALCODE || '
Country: ' || a.BILLING_COUNTRY || '
Industry: ' || a.INDUSTRY || '

CONTRACT DETAILS:
Term: ' || c.CONTRACT_TERM || ' months
Auto-Renewal: ' || CASE WHEN c.AUTO_RENEW THEN 'Yes' ELSE 'No' END || '
Currency: ' || c.CURRENCY_ISO_CODE || '

LICENSED PRODUCTS AND SERVICES:
' || (
    SELECT LISTAGG(
        '- ' || p.PRODUCT_NAME || ' (' || p.PRODUCT_CODE || '): ' || 
        s.QUANTITY::VARCHAR || ' users @ $' || s.CUSTOMER_PRICE::VARCHAR || '/user/month' ||
        CASE WHEN s.DISCOUNT > 0 THEN ' (' || s.DISCOUNT::VARCHAR || '% discount)' ELSE '' END,
        '\n'
    ) WITHIN GROUP (ORDER BY p.PRODUCT_NAME)
    FROM PROD.RAW.SFDC_SUBSCRIPTION_CPQ s
    JOIN PROD.RAW.SFDC_PRODUCT p ON s.PRODUCT_ID = p.PRODUCT_ID
    WHERE s.CONTRACT_ID = c.CONTRACT_ID
) || '

PRICING SUMMARY:
Monthly Recurring Revenue (MRR): $' || c.MRR::VARCHAR || '
Annual Recurring Revenue (ARR): $' || c.ARR::VARCHAR || '
Total Contract Value (TCV): $' || c.TCV::VARCHAR || '

TERMS AND CONDITIONS:
1. Grant of License: Provider grants Customer a non-exclusive license to use the products listed above.
2. Payment Terms: Net 30 days from invoice date.
3. Data Protection: Provider will process Customer data in accordance with applicable data protection laws.
4. Service Level: Provider commits to 99.9% uptime availability.
5. Support: 24/7 technical support included.

SIGNATURES:
Customer: ' || c.CUSTOMER_SIGNED_TITLE || ' (Signed: ' || c.CUSTOMER_SIGNED_DATE::VARCHAR || ')
Provider: VP of Sales (Signed: ' || c.ACTIVATED_DATE::DATE::VARCHAR || ')

This contract is confidential and proprietary.' AS contract_text,
    -- Summary
    'Identity services agreement with ' || a.ACCOUNT_NAME || ' for ' || c.CONTRACT_TERM || ' months. Total value: $' || c.TCV::VARCHAR AS contract_summary,
    -- Products list
    (
        SELECT LISTAGG(p.PRODUCT_NAME, ', ') WITHIN GROUP (ORDER BY p.PRODUCT_NAME)
        FROM PROD.RAW.SFDC_SUBSCRIPTION_CPQ s
        JOIN PROD.RAW.SFDC_PRODUCT p ON s.PRODUCT_ID = p.PRODUCT_ID
        WHERE s.CONTRACT_ID = c.CONTRACT_ID
    ) AS products_list,
    c.TCV AS total_value,
    CURRENT_TIMESTAMP()
FROM PROD.RAW.SFDC_CONTRACT c
JOIN PROD.RAW.SFDC_ACCOUNT a ON c.ACCOUNT_ID = a.ACCOUNT_ID;

-- Verify content
SELECT COUNT(*) FROM PROD.FINAL.CONTRACT_CONTENT;
