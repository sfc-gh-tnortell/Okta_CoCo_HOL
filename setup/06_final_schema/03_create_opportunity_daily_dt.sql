-- ============================================================
-- Step 6d: Create OPPORTUNITY_DAILY Dynamic Table
-- ============================================================
-- Transforms opportunity data with account and product enrichment

CREATE OR REPLACE DYNAMIC TABLE PROD.FINAL.OPPORTUNITY_DAILY
    TARGET_LAG = '1 day'
    WAREHOUSE = COMPUTE_WH
AS
SELECT 
    o.OPPORTUNITY_ID,
    o.OPPORTUNITY_NAME,
    o.ACCOUNT_ID,
    a.ACCOUNT_NAME,
    o.CONTRACT_ID,
    o.PRODUCT_ID,
    p.PRODUCT_NAME,
    p.PRODUCT_CODE,
    p.PRODUCT_FAMILY,
    o.STAGE,
    o.STATUS,
    o.AMOUNT,
    o.CLOSE_DATE,
    o.CREATED_DATE,
    o.LOSS_REASON,
    o.COMPETITOR,
    o.NEXT_STEPS,
    o.DESCRIPTION,
    -- Account context
    a.TERRITORY,
    a.TIMEZONE,
    a.INDUSTRY,
    a.HEALTHSCORE,
    st.ACCOUNT_EXECUTIVE,
    -- Calculated fields
    DATEDIFF(day, o.CREATED_DATE, o.CLOSE_DATE) AS DAYS_TO_CLOSE,
    CASE 
        WHEN o.STAGE = 'Closed Lost' THEN 'Lost'
        WHEN o.STAGE = 'Closed Won' THEN 'Won'
        ELSE 'Open'
    END AS OPPORTUNITY_STATUS,
    CURRENT_TIMESTAMP() AS UPDATED_AT
FROM PROD.RAW.SFDC_OPPORTUNITY o
JOIN PROD.RAW.SFDC_ACCOUNT a ON o.ACCOUNT_ID = a.ACCOUNT_ID
JOIN PROD.RAW.SFDC_PRODUCT p ON o.PRODUCT_ID = p.PRODUCT_ID
LEFT JOIN PROD.RAW.SALES_TEAM st ON a.SALES_TEAM_ID = st.TEAM_ID;

-- Verify dynamic table
SELECT STAGE, COUNT(*) as count FROM PROD.FINAL.OPPORTUNITY_DAILY GROUP BY STAGE;
