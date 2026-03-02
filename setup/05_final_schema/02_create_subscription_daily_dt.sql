-- ============================================================
-- Step 6c: Create SUBSCRIPTION_DAILY Dynamic Table
-- ============================================================
-- Transforms raw subscription data with product enrichment

USE ROLE SYSADMIN;
USE WAREHOUSE DEFAULT_WH;

CREATE OR REPLACE DYNAMIC TABLE PROD.FINAL.SUBSCRIPTION_DAILY
    TARGET_LAG = '1 day'
    WAREHOUSE = DEFAULT_WH
AS
SELECT 
    s.SUBSCRIPTION_ID,
    s.SUBSCRIPTION_NAME,
    s.ACCOUNT_ID,
    a.ACCOUNT_NAME,
    s.CONTRACT_ID,
    s.CONTRACT_NUMBER,
    s.PRODUCT_ID,
    p.PRODUCT_NAME,
    p.PRODUCT_CODE,
    p.PRODUCT_FAMILY,
    p.PRODUCT_CATEGORY,
    s.CURRENCY_ISO_CODE,
    s.START_DATE,
    s.END_DATE,
    s.QUANTITY,
    s.LIST_PRICE,
    s.DISCOUNT,
    s.CUSTOMER_PRICE,
    s.NET_PRICE,
    s.ARR,
    s.MRR,
    s.TCV,
    -- Account context
    a.TERRITORY,
    a.TIMEZONE,
    a.INDUSTRY,
    a.HEALTHSCORE,
    -- Calculated fields
    DATEDIFF(day, CURRENT_DATE(), s.END_DATE) AS DAYS_TO_EXPIRY,
    CASE 
        WHEN s.DISCOUNT > 20 THEN 'High Discount'
        WHEN s.DISCOUNT > 10 THEN 'Medium Discount'
        WHEN s.DISCOUNT > 0 THEN 'Low Discount'
        ELSE 'No Discount'
    END AS DISCOUNT_TIER,
    ROUND(s.DISCOUNT - 15, 2) AS DISCOUNT_VS_TARGET,
    CURRENT_TIMESTAMP() AS UPDATED_AT
FROM PROD.RAW.SFDC_SUBSCRIPTION_CPQ s
JOIN PROD.RAW.SFDC_ACCOUNT a ON s.ACCOUNT_ID = a.ACCOUNT_ID
JOIN PROD.RAW.SFDC_PRODUCT p ON s.PRODUCT_ID = p.PRODUCT_ID;

-- Verify dynamic table
SELECT COUNT(*) FROM PROD.FINAL.SUBSCRIPTION_DAILY;
