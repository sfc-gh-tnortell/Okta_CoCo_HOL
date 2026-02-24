-- ============================================================
-- Step 8: Create Semantic View for Cortex Analyst
-- ============================================================
-- Enables natural language queries over structured data

CREATE OR REPLACE SEMANTIC VIEW PROD.FINAL.CUSTOMER_360_SEMANTIC_VIEW
COMMENT = 'Customer 360 semantic view for identity management SaaS business. Enables natural language queries about accounts, subscriptions, opportunities, and sales performance.'

TABLES (
    PROD.FINAL.ACCOUNT_DAILY AS accounts
        COMMENT = 'Customer accounts with sales team assignments and renewal information'
        PRIMARY KEY (ACCOUNT_ID)
        WITH COLUMNS (
            ACCOUNT_ID COMMENT = 'Unique account identifier',
            ACCOUNT_NAME COMMENT = 'Customer company name (Fortune 500 companies)',
            ACCOUNT_STATUS COMMENT = 'Account status: Active or Churned',
            CUSTOMER_ACQUISITION_DATE COMMENT = 'Date customer was acquired',
            RENEWAL_DATE COMMENT = 'Contract renewal date',
            CARR COMMENT = 'Contracted Annual Recurring Revenue',
            CARR_USD COMMENT = 'CARR in US Dollars',
            BILLING_CITY COMMENT = 'Customer headquarters city',
            BILLING_STATE COMMENT = 'Customer headquarters state',
            TERRITORY COMMENT = 'Sales territory: West, Mountain, Central, East',
            TIMEZONE COMMENT = 'Customer timezone: Pacific, Mountain, Central, Eastern',
            INDUSTRY COMMENT = 'Customer industry vertical',
            SUB_INDUSTRY COMMENT = 'Customer sub-industry',
            ANNUAL_REVENUE COMMENT = 'Customer annual revenue',
            NUMBER_OF_EMPLOYEES COMMENT = 'Customer employee count',
            HEALTHSCORE COMMENT = 'Account health: Excellent, Healthy, Good, At Risk, Critical',
            TOP_ACCOUNT COMMENT = 'Strategic top account flag',
            NAMED_ACCOUNT COMMENT = 'Named account flag',
            ACCOUNT_EXECUTIVE COMMENT = 'Assigned account executive name',
            SALES_ENGINEER COMMENT = 'Assigned sales engineer name',
            SDR COMMENT = 'Assigned sales development representative',
            DAYS_TO_RENEWAL COMMENT = 'Days until contract renewal',
            RENEWAL_URGENCY COMMENT = 'Renewal urgency: Immediate, Near Term, Medium Term, Long Term'
        ),
    
    PROD.FINAL.SUBSCRIPTION_DAILY AS subscriptions
        COMMENT = 'Product subscriptions with pricing and discount information'
        PRIMARY KEY (SUBSCRIPTION_ID)
        WITH COLUMNS (
            SUBSCRIPTION_ID COMMENT = 'Unique subscription identifier',
            ACCOUNT_ID COMMENT = 'Parent account identifier',
            ACCOUNT_NAME COMMENT = 'Customer company name',
            CONTRACT_ID COMMENT = 'Parent contract identifier',
            PRODUCT_ID COMMENT = 'Product identifier',
            PRODUCT_NAME COMMENT = 'Identity product name (SSO, MFA, Lifecycle Management, etc.)',
            PRODUCT_CODE COMMENT = 'Product code (SSO, MFA, LCM, PAM, etc.)',
            PRODUCT_FAMILY COMMENT = 'Product family grouping',
            PRODUCT_CATEGORY COMMENT = 'Product category (Security, Core, Automation, Governance)',
            START_DATE COMMENT = 'Subscription start date',
            END_DATE COMMENT = 'Subscription end date',
            QUANTITY COMMENT = 'Number of licensed users',
            LIST_PRICE COMMENT = 'List price per user per month',
            DISCOUNT COMMENT = 'Discount percentage applied',
            CUSTOMER_PRICE COMMENT = 'Price after discount per user per month',
            ARR COMMENT = 'Annual recurring revenue for this subscription',
            MRR COMMENT = 'Monthly recurring revenue for this subscription',
            TERRITORY COMMENT = 'Sales territory',
            INDUSTRY COMMENT = 'Customer industry',
            HEALTHSCORE COMMENT = 'Account health score',
            DISCOUNT_TIER COMMENT = 'Discount classification: High, Medium, Low, No Discount',
            DISCOUNT_VS_TARGET COMMENT = 'Discount variance from 15% target (positive = above target)'
        ),
    
    PROD.FINAL.OPPORTUNITY_DAILY AS opportunities
        COMMENT = 'Sales opportunities including expansion attempts and lost deals'
        PRIMARY KEY (OPPORTUNITY_ID)
        WITH COLUMNS (
            OPPORTUNITY_ID COMMENT = 'Unique opportunity identifier',
            OPPORTUNITY_NAME COMMENT = 'Opportunity description',
            ACCOUNT_ID COMMENT = 'Parent account identifier',
            ACCOUNT_NAME COMMENT = 'Customer company name',
            PRODUCT_ID COMMENT = 'Product being proposed',
            PRODUCT_NAME COMMENT = 'Product name being proposed',
            PRODUCT_CODE COMMENT = 'Product code being proposed',
            STAGE COMMENT = 'Opportunity stage: Closed Won or Closed Lost',
            STATUS COMMENT = 'Win/Loss status',
            AMOUNT COMMENT = 'Opportunity value in dollars',
            CLOSE_DATE COMMENT = 'Date opportunity was closed',
            LOSS_REASON COMMENT = 'Reason for lost opportunity (budget, competitor, timing, etc.)',
            COMPETITOR COMMENT = 'Competitor that won the deal if lost',
            NEXT_STEPS COMMENT = 'Planned next steps',
            TERRITORY COMMENT = 'Sales territory',
            INDUSTRY COMMENT = 'Customer industry',
            HEALTHSCORE COMMENT = 'Account health at time of opportunity',
            DAYS_TO_CLOSE COMMENT = 'Days from creation to close'
        ),
    
    PROD.RAW.SFDC_PRODUCT AS products
        COMMENT = 'Identity management product catalog'
        PRIMARY KEY (PRODUCT_ID)
        WITH COLUMNS (
            PRODUCT_ID COMMENT = 'Unique product identifier',
            PRODUCT_NAME COMMENT = 'Product name',
            PRODUCT_CODE COMMENT = 'Short product code',
            PRODUCT_DESCRIPTION COMMENT = 'Product description',
            PRODUCT_FAMILY COMMENT = 'Product family',
            PRODUCT_CATEGORY COMMENT = 'Product category',
            LIST_PRICE_USD COMMENT = 'List price per user per month in USD',
            IS_ACTIVE COMMENT = 'Product availability flag'
        ),
    
    PROD.RAW.SALES_TEAM AS sales_teams
        COMMENT = 'Sales team assignments by territory'
        PRIMARY KEY (TEAM_ID)
        WITH COLUMNS (
            TEAM_ID COMMENT = 'Unique team identifier',
            TERRITORY COMMENT = 'Sales territory',
            TIMEZONE COMMENT = 'Territory timezone',
            REGION COMMENT = 'Geographic region',
            ACCOUNT_EXECUTIVE COMMENT = 'Account executive name',
            SALES_ENGINEER COMMENT = 'Sales engineer name',
            SDR COMMENT = 'Sales development representative name'
        )
)

RELATIONSHIPS (
    subscriptions (ACCOUNT_ID) REFERENCES accounts (ACCOUNT_ID)
        COMMENT = 'Subscriptions belong to accounts',
    opportunities (ACCOUNT_ID) REFERENCES accounts (ACCOUNT_ID)
        COMMENT = 'Opportunities belong to accounts',
    subscriptions (PRODUCT_ID) REFERENCES products (PRODUCT_ID)
        COMMENT = 'Subscriptions are for specific products',
    opportunities (PRODUCT_ID) REFERENCES products (PRODUCT_ID)
        COMMENT = 'Opportunities propose specific products'
)

FACTS (
    accounts (
        FACT total_accounts COMMENT = 'Count of accounts' AS COUNT(ACCOUNT_ID),
        FACT total_carr COMMENT = 'Total CARR across accounts' AS SUM(CARR_USD),
        FACT avg_carr COMMENT = 'Average CARR per account' AS AVG(CARR_USD),
        FACT active_accounts COMMENT = 'Count of active accounts' AS COUNT_IF(ACCOUNT_STATUS = 'Active'),
        FACT churned_accounts COMMENT = 'Count of churned accounts' AS COUNT_IF(ACCOUNT_STATUS = 'Churned'),
        FACT at_risk_accounts COMMENT = 'Accounts with At Risk or Critical health' AS COUNT_IF(HEALTHSCORE IN ('At Risk', 'Critical')),
        FACT renewals_next_30_days COMMENT = 'Accounts renewing in next 30 days' AS COUNT_IF(DAYS_TO_RENEWAL <= 30),
        FACT renewals_next_90_days COMMENT = 'Accounts renewing in next 90 days' AS COUNT_IF(DAYS_TO_RENEWAL <= 90)
    ),
    subscriptions (
        FACT total_subscriptions COMMENT = 'Count of subscriptions' AS COUNT(SUBSCRIPTION_ID),
        FACT total_arr COMMENT = 'Total ARR from subscriptions' AS SUM(ARR),
        FACT total_mrr COMMENT = 'Total MRR from subscriptions' AS SUM(MRR),
        FACT avg_discount COMMENT = 'Average discount percentage' AS AVG(DISCOUNT),
        FACT total_users COMMENT = 'Total licensed users' AS SUM(QUANTITY),
        FACT high_discount_subs COMMENT = 'Subscriptions with >20% discount' AS COUNT_IF(DISCOUNT > 20)
    ),
    opportunities (
        FACT total_opportunities COMMENT = 'Count of opportunities' AS COUNT(OPPORTUNITY_ID),
        FACT total_pipeline COMMENT = 'Total opportunity value' AS SUM(AMOUNT),
        FACT won_opportunities COMMENT = 'Count of won opportunities' AS COUNT_IF(STAGE = 'Closed Won'),
        FACT lost_opportunities COMMENT = 'Count of lost opportunities' AS COUNT_IF(STAGE = 'Closed Lost'),
        FACT win_rate COMMENT = 'Win rate percentage' AS COUNT_IF(STAGE = 'Closed Won') * 100.0 / NULLIF(COUNT(OPPORTUNITY_ID), 0),
        FACT lost_to_competitor COMMENT = 'Deals lost to competitors' AS COUNT_IF(COMPETITOR IS NOT NULL),
        FACT avg_deal_size COMMENT = 'Average opportunity amount' AS AVG(AMOUNT)
    )
)

DIMENSIONS (
    accounts (
        DIMENSION account_status COMMENT = 'Active or Churned' AS ACCOUNT_STATUS,
        DIMENSION territory COMMENT = 'Sales territory' AS TERRITORY,
        DIMENSION timezone COMMENT = 'Customer timezone' AS TIMEZONE,
        DIMENSION industry COMMENT = 'Industry vertical' AS INDUSTRY,
        DIMENSION healthscore COMMENT = 'Account health' AS HEALTHSCORE,
        DIMENSION renewal_urgency COMMENT = 'Renewal urgency level' AS RENEWAL_URGENCY,
        DIMENSION is_top_account COMMENT = 'Top account flag' AS TOP_ACCOUNT,
        DIMENSION account_executive COMMENT = 'Assigned AE' AS ACCOUNT_EXECUTIVE
    ),
    subscriptions (
        DIMENSION product_name COMMENT = 'Product name' AS PRODUCT_NAME,
        DIMENSION product_code COMMENT = 'Product code' AS PRODUCT_CODE,
        DIMENSION product_category COMMENT = 'Product category' AS PRODUCT_CATEGORY,
        DIMENSION discount_tier COMMENT = 'Discount tier' AS DISCOUNT_TIER
    ),
    opportunities (
        DIMENSION stage COMMENT = 'Won or Lost' AS STAGE,
        DIMENSION loss_reason COMMENT = 'Reason for loss' AS LOSS_REASON,
        DIMENSION competitor COMMENT = 'Winning competitor' AS COMPETITOR
    )
)

METRICS (
    METRIC churn_rate 
        COMMENT = 'Percentage of churned accounts' 
        AS churned_accounts * 100.0 / NULLIF(total_accounts, 0),
    METRIC avg_products_per_account 
        COMMENT = 'Average products per account' 
        AS total_subscriptions * 1.0 / NULLIF(total_accounts, 0),
    METRIC revenue_per_user 
        COMMENT = 'ARR per licensed user' 
        AS total_arr / NULLIF(total_users, 0),
    METRIC discount_efficiency 
        COMMENT = 'Revenue generated per discount point' 
        AS total_arr / NULLIF(avg_discount, 0)
);

-- Verify semantic view
DESCRIBE SEMANTIC VIEW PROD.FINAL.CUSTOMER_360_SEMANTIC_VIEW;
