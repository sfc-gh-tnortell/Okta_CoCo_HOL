-- ============================================================
-- Step 3f: Generate Failed Expansion Opportunities
-- ============================================================
-- Creates opportunities including failed expansion attempts for products
-- that were proposed but declined by the customer

USE ROLE SYSADMIN;
USE WAREHOUSE DEFAULT_WH;

INSERT INTO PROD.RAW.SFDC_OPPORTUNITY
WITH existing_subscriptions AS (
    -- Get products each account already has
    SELECT DISTINCT ACCOUNT_ID, PRODUCT_ID
    FROM PROD.RAW.SFDC_SUBSCRIPTION_CPQ
),
potential_expansions AS (
    -- For each account, identify products they DON'T have yet
    SELECT 
        a.ACCOUNT_ID,
        a.ACCOUNT_NAME,
        p.PRODUCT_ID,
        p.PRODUCT_NAME,
        p.LIST_PRICE_USD
    FROM PROD.RAW.SFDC_ACCOUNT a
    CROSS JOIN PROD.RAW.SFDC_PRODUCT p
    LEFT JOIN existing_subscriptions es 
        ON a.ACCOUNT_ID = es.ACCOUNT_ID AND p.PRODUCT_ID = es.PRODUCT_ID
    WHERE es.PRODUCT_ID IS NULL  -- Only products they don't have
),
numbered_opportunities AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (ORDER BY ACCOUNT_ID, PRODUCT_ID) AS opp_num,
        -- Assign status: 60% Closed Won, 40% Closed Lost
        CASE WHEN MOD(ABS(HASH(ACCOUNT_ID || PRODUCT_ID)), 100) < 40 THEN 'Closed Lost' ELSE 'Closed Won' END AS stage
    FROM potential_expansions
)
SELECT 
    'OPP' || LPAD(opp_num::VARCHAR, 7, '0') AS opportunity_id,
    ACCOUNT_NAME || ' - ' || PRODUCT_NAME || ' Expansion' AS opportunity_name,
    ACCOUNT_ID,
    NULL AS contract_id,
    PRODUCT_ID,
    stage,
    CASE WHEN stage = 'Closed Won' THEN 'Won' ELSE 'Lost' END AS status,
    ROUND(LIST_PRICE_USD * (500 + MOD(ABS(HASH(ACCOUNT_ID || PRODUCT_ID || 'qty')), 4500)) * 12, 2) AS amount,
    DATEADD(day, -MOD(ABS(HASH(ACCOUNT_ID || PRODUCT_ID || 'close')), 365), CURRENT_DATE()) AS close_date,
    DATEADD(day, -MOD(ABS(HASH(ACCOUNT_ID || PRODUCT_ID || 'create')), 400) - 30, CURRENT_DATE()) AS created_date,
    CASE WHEN stage = 'Closed Lost' THEN
        CASE MOD(ABS(HASH(ACCOUNT_ID || PRODUCT_ID || 'loss')), 8)
            WHEN 0 THEN 'Budget constraints - fiscal year budget already allocated'
            WHEN 1 THEN 'Competitor selected - chose alternative vendor'
            WHEN 2 THEN 'Project deprioritized - shifting focus to other initiatives'
            WHEN 3 THEN 'Internal resources - building in-house solution'
            WHEN 4 THEN 'Timing - not ready for this capability yet'
            WHEN 5 THEN 'Price too high - discount offered was insufficient'
            WHEN 6 THEN 'Organizational changes - key sponsor left the company'
            ELSE 'Technical requirements not met'
        END
    ELSE NULL END AS loss_reason,
    CASE WHEN stage = 'Closed Lost' THEN
        CASE MOD(ABS(HASH(ACCOUNT_ID || PRODUCT_ID || 'comp')), 6)
            WHEN 0 THEN 'Microsoft Entra ID'
            WHEN 1 THEN 'Ping Identity'
            WHEN 2 THEN 'ForgeRock'
            WHEN 3 THEN 'Auth0'
            WHEN 4 THEN 'OneLogin'
            ELSE 'CyberArk'
        END
    ELSE NULL END AS competitor,
    CASE WHEN stage = 'Closed Won' THEN 'Schedule implementation kickoff'
         ELSE 'Re-engage in Q' || (MOD(ABS(HASH(ACCOUNT_ID || PRODUCT_ID)), 4) + 1)::VARCHAR || ' next fiscal year'
    END AS next_steps,
    'Expansion opportunity for ' || PRODUCT_NAME || ' presented to ' || ACCOUNT_NAME || 
    CASE WHEN stage = 'Closed Lost' THEN '. Customer declined due to ' || 
        CASE MOD(ABS(HASH(ACCOUNT_ID || PRODUCT_ID || 'loss')), 8)
            WHEN 0 THEN 'budget constraints.'
            WHEN 1 THEN 'competitive pressure.'
            WHEN 2 THEN 'shifting priorities.'
            WHEN 3 THEN 'internal build decision.'
            WHEN 4 THEN 'timing issues.'
            WHEN 5 THEN 'pricing concerns.'
            WHEN 6 THEN 'organizational changes.'
            ELSE 'technical gaps.'
        END
    ELSE '. Successfully closed and moving to implementation.'
    END AS description
FROM numbered_opportunities
WHERE opp_num <= 500;  -- Limit to 500 opportunities

-- Summary of opportunities
SELECT 
    stage,
    COUNT(*) as count,
    ROUND(AVG(amount), 2) as avg_amount
FROM PROD.RAW.SFDC_OPPORTUNITY
GROUP BY stage;

-- Lost opportunities by reason
SELECT 
    loss_reason,
    COUNT(*) as count
FROM PROD.RAW.SFDC_OPPORTUNITY
WHERE stage = 'Closed Lost'
GROUP BY loss_reason
ORDER BY count DESC;
