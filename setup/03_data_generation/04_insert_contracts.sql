-- ============================================================
-- Step 3d: Generate Contracts (1 per Account)
-- ============================================================

INSERT INTO PROD.RAW.SFDC_CONTRACT
SELECT 
    'CON' || LPAD(ROW_NUMBER() OVER (ORDER BY ACCOUNT_ID)::VARCHAR, 6, '0') AS contract_id,
    'CON-' || YEAR(CURRENT_DATE()) || '-' || LPAD(ROW_NUMBER() OVER (ORDER BY ACCOUNT_ID)::VARCHAR, 6, '0') AS contract_number,
    ACCOUNT_ID,
    'USD' AS currency_iso_code,
    DATEADD(day, -MOD(ABS(HASH(ACCOUNT_ID)), 365), CURRENT_DATE()) AS start_date,
    DATEADD(month, 12, DATEADD(day, -MOD(ABS(HASH(ACCOUNT_ID)), 365), CURRENT_DATE())) AS end_date,
    12 AS contract_term,
    CASE WHEN ACCOUNT_STATUS = 'Active' THEN 'Activated' ELSE 'Expired' END AS contract_status,
    BILLING_STREET,
    BILLING_CITY,
    BILLING_STATE,
    BILLING_POSTALCODE,
    BILLING_COUNTRY,
    CARR_USD AS tcv,
    CARR_USD AS carr,
    ROUND(CARR_USD / 12, 2) AS mrr,
    CARR_USD AS arr,
    CREATED_DATE,
    DATEADD(day, MOD(ABS(HASH(ACCOUNT_ID || 'act')), 14) + 1, CREATED_DATE) AS activated_date,
    CASE WHEN MOD(ABS(HASH(ACCOUNT_ID || 'renew')), 100) < 70 THEN TRUE ELSE FALSE END AS auto_renew,
    CREATED_DATE::DATE AS customer_signed_date,
    CASE MOD(ABS(HASH(ACCOUNT_ID || 'title')), 7)
        WHEN 0 THEN 'CEO'
        WHEN 1 THEN 'CTO'
        WHEN 2 THEN 'CFO'
        WHEN 3 THEN 'VP IT'
        WHEN 4 THEN 'Director'
        WHEN 5 THEN 'IT Manager'
        ELSE 'CISO'
    END AS customer_signed_title
FROM PROD.RAW.SFDC_ACCOUNT;

SELECT COUNT(*) AS contract_count FROM PROD.RAW.SFDC_CONTRACT;
