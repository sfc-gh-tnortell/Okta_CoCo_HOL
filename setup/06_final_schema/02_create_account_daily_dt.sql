-- ============================================================
-- Step 6b: Create ACCOUNT_DAILY Dynamic Table
-- ============================================================
-- Transforms raw account data with sales team enrichment

CREATE OR REPLACE DYNAMIC TABLE PROD.FINAL.ACCOUNT_DAILY
    TARGET_LAG = '1 day'
    WAREHOUSE = COMPUTE_WH
AS
SELECT 
    a.ACCOUNT_ID,
    a.ACCOUNT_NAME,
    a.ACCOUNT_STATUS,
    a.ACCOUNT_TYPE,
    a.CREATED_DATE,
    a.CUSTOMER_ACQUISITION_DATE,
    a.RENEWAL_DATE,
    a.CARR,
    a.CARR_USD,
    a.BILLING_CITY,
    a.BILLING_STATE,
    a.BILLING_COUNTRY,
    a.GEOGRAPHY,
    a.TERRITORY,
    a.TIMEZONE,
    a.INDUSTRY,
    a.SUB_INDUSTRY,
    a.ANNUAL_REVENUE,
    a.NUMBER_OF_EMPLOYEES,
    a.HEALTHSCORE,
    a.TOP_ACCOUNT,
    a.NAMED_ACCOUNT,
    -- Sales team info
    st.TEAM_ID,
    st.ACCOUNT_EXECUTIVE,
    st.SALES_ENGINEER,
    st.SDR,
    st.REGION,
    -- Calculated fields
    DATEDIFF(day, CURRENT_DATE(), a.RENEWAL_DATE) AS DAYS_TO_RENEWAL,
    CASE 
        WHEN DATEDIFF(day, CURRENT_DATE(), a.RENEWAL_DATE) <= 30 THEN 'Immediate'
        WHEN DATEDIFF(day, CURRENT_DATE(), a.RENEWAL_DATE) <= 90 THEN 'Near Term'
        WHEN DATEDIFF(day, CURRENT_DATE(), a.RENEWAL_DATE) <= 180 THEN 'Medium Term'
        ELSE 'Long Term'
    END AS RENEWAL_URGENCY,
    CURRENT_TIMESTAMP() AS UPDATED_AT
FROM PROD.RAW.SFDC_ACCOUNT a
LEFT JOIN PROD.RAW.SALES_TEAM st 
    ON a.TERRITORY = st.TERRITORY
    AND MOD(ABS(HASH(a.ACCOUNT_ID)), 
        (SELECT COUNT(*) FROM PROD.RAW.SALES_TEAM WHERE TERRITORY = a.TERRITORY)) = 
        MOD(ABS(HASH(st.TEAM_ID)), 
        (SELECT COUNT(*) FROM PROD.RAW.SALES_TEAM WHERE TERRITORY = a.TERRITORY));

-- Verify dynamic table
SELECT COUNT(*) FROM PROD.FINAL.ACCOUNT_DAILY;
