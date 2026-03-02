-- ============================================================
-- Step 6b: Create ACCOUNT_DAILY Dynamic Table
-- ============================================================
-- Transforms raw account data with sales team enrichment
-- Each account is assigned to exactly ONE sales team (1:1 relationship)

USE ROLE SYSADMIN;
USE WAREHOUSE DEFAULT_WH;

CREATE OR REPLACE DYNAMIC TABLE PROD.FINAL.ACCOUNT_DAILY
    TARGET_LAG = '1 day'
    WAREHOUSE = DEFAULT_WH
AS
WITH ranked_teams AS (
    -- Assign a row number to each team within their territory
    SELECT 
        TEAM_ID,
        TERRITORY,
        ACCOUNT_EXECUTIVE,
        SALES_ENGINEER,
        SDR,
        REGION,
        ROW_NUMBER() OVER (PARTITION BY TERRITORY ORDER BY TEAM_ID) - 1 AS team_rank,
        COUNT(*) OVER (PARTITION BY TERRITORY) AS teams_in_territory
    FROM PROD.RAW.SALES_TEAM
),
accounts_with_assignment AS (
    -- Assign each account to a single team using modulo on account hash
    SELECT 
        a.*,
        MOD(ABS(HASH(a.ACCOUNT_ID)), COALESCE(
            (SELECT MAX(teams_in_territory) FROM ranked_teams rt WHERE rt.TERRITORY = a.TERRITORY), 1
        )) AS assigned_team_rank
    FROM PROD.RAW.SFDC_ACCOUNT a
)
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
    -- Sales team info (1:1 assignment)
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
FROM accounts_with_assignment a
LEFT JOIN ranked_teams st 
    ON a.TERRITORY = st.TERRITORY
    AND a.assigned_team_rank = st.team_rank;

-- Verify dynamic table has 1:1 relationship (no duplicates)
SELECT ACCOUNT_ID, COUNT(*) as cnt 
FROM PROD.FINAL.ACCOUNT_DAILY 
GROUP BY ACCOUNT_ID 
HAVING cnt > 1;
