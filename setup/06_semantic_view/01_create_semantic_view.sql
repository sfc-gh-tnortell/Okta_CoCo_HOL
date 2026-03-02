-- ============================================================
-- Step 7: Create Semantic View for Cortex Analyst
-- ============================================================
-- Enables natural language queries over structured data
--
-- Two options are provided:
--   Option A: SQL syntax (CREATE SEMANTIC VIEW)
--   Option B: YAML syntax (SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML)

USE ROLE SYSADMIN;
USE WAREHOUSE DEFAULT_WH;

-- ============================================================
-- OPTION A: SQL Syntax
-- ============================================================

CREATE OR REPLACE SEMANTIC VIEW PROD.FINAL.CUSTOMER_360_SEMANTIC_VIEW

  TABLES (
    accounts AS PROD.FINAL.ACCOUNT_DAILY
      PRIMARY KEY (ACCOUNT_ID)
      WITH SYNONYMS ('customers', 'clients')
      COMMENT = 'Customer accounts with sales team assignments and renewal information',
    
    subscriptions AS PROD.FINAL.SUBSCRIPTION_DAILY
      PRIMARY KEY (SUBSCRIPTION_ID)
      WITH SYNONYMS ('licenses', 'products owned')
      COMMENT = 'Product subscriptions with pricing and discount information',
    
    opportunities AS PROD.FINAL.OPPORTUNITY_DAILY
      PRIMARY KEY (OPPORTUNITY_ID)
      WITH SYNONYMS ('deals', 'sales')
      COMMENT = 'Sales opportunities including expansion attempts and lost deals',
    
    products AS PROD.RAW.SFDC_PRODUCT
      PRIMARY KEY (PRODUCT_ID)
      COMMENT = 'Identity management product catalog'
  )

  RELATIONSHIPS (
    subscriptions_to_accounts AS
      subscriptions (ACCOUNT_ID) REFERENCES accounts,
    opportunities_to_accounts AS
      opportunities (ACCOUNT_ID) REFERENCES accounts,
    subscriptions_to_products AS
      subscriptions (PRODUCT_ID) REFERENCES products,
    opportunities_to_products AS
      opportunities (PRODUCT_ID) REFERENCES products
  )

  FACTS (
    accounts.carr_value AS CARR_USD
      COMMENT = 'Contracted Annual Recurring Revenue in USD',
    accounts.employee_count AS NUMBER_OF_EMPLOYEES
      COMMENT = 'Customer employee count',
    accounts.revenue AS ANNUAL_REVENUE
      COMMENT = 'Customer annual revenue',
    subscriptions.arr_value AS ARR
      COMMENT = 'Annual recurring revenue for subscription',
    subscriptions.mrr_value AS MRR
      COMMENT = 'Monthly recurring revenue for subscription',
    subscriptions.user_count AS QUANTITY
      COMMENT = 'Number of licensed users',
    subscriptions.discount_pct AS DISCOUNT
      COMMENT = 'Discount percentage applied',
    opportunities.deal_value AS AMOUNT
      COMMENT = 'Opportunity value in dollars',
    opportunities.close_duration AS DAYS_TO_CLOSE
      COMMENT = 'Days from creation to close'
  )

  DIMENSIONS (
    accounts.account_name AS ACCOUNT_NAME
      WITH SYNONYMS = ('customer name', 'company name')
      COMMENT = 'Customer company name',
    accounts.account_status AS ACCOUNT_STATUS
      COMMENT = 'Account status: Active or Churned',
    accounts.territory AS TERRITORY
      COMMENT = 'Sales territory: West, Mountain, Central, East',
    accounts.timezone AS TIMEZONE
      COMMENT = 'Customer timezone',
    accounts.industry AS INDUSTRY
      COMMENT = 'Customer industry vertical',
    accounts.healthscore AS HEALTHSCORE
      WITH SYNONYMS = ('health', 'account health')
      COMMENT = 'Account health: Excellent, Healthy, Good, At Risk, Critical',
    accounts.account_executive AS ACCOUNT_EXECUTIVE
      WITH SYNONYMS = ('AE', 'rep', 'sales rep')
      COMMENT = 'Assigned account executive',
    accounts.renewal_date AS RENEWAL_DATE
      COMMENT = 'Contract renewal date',
    accounts.days_to_renewal AS DAYS_TO_RENEWAL
      COMMENT = 'Days until contract renewal',
    
    subscriptions.product_name AS subscriptions.PRODUCT_NAME
      WITH SYNONYMS = ('product', 'service')
      COMMENT = 'Identity product name',
    subscriptions.product_code AS subscriptions.PRODUCT_CODE
      COMMENT = 'Product code (SSO, MFA, LCM, PAM, etc.)',
    
    opportunities.stage AS STAGE
      COMMENT = 'Opportunity stage: Closed Won or Closed Lost',
    opportunities.loss_reason AS LOSS_REASON
      WITH SYNONYMS = ('why lost', 'reason for loss')
      COMMENT = 'Reason for lost opportunity',
    opportunities.competitor AS COMPETITOR
      COMMENT = 'Competitor that won the deal',
    opportunities.close_date AS CLOSE_DATE
      COMMENT = 'Date opportunity was closed',
    
    products.product_name AS products.PRODUCT_NAME
      COMMENT = 'Product catalog name',
    products.product_family AS PRODUCT_FAMILY
      COMMENT = 'Product family grouping',
    products.list_price_usd AS LIST_PRICE_USD
      COMMENT = 'List price per user per month'
  )

  METRICS (
    accounts.total_accounts AS COUNT(ACCOUNT_ID)
      COMMENT = 'Count of accounts',
    accounts.total_carr AS SUM(accounts.carr_value)
      COMMENT = 'Total CARR across accounts',
    accounts.avg_carr AS AVG(accounts.carr_value)
      COMMENT = 'Average CARR per account',
    accounts.active_accounts AS COUNT_IF(ACCOUNT_STATUS = 'Active')
      COMMENT = 'Count of active accounts',
    accounts.churned_accounts AS COUNT_IF(ACCOUNT_STATUS = 'Churned')
      COMMENT = 'Count of churned accounts',
    accounts.at_risk_accounts AS COUNT_IF(HEALTHSCORE IN ('At Risk', 'Critical'))
      COMMENT = 'Accounts with At Risk or Critical health',
    
    subscriptions.total_subscriptions AS COUNT(SUBSCRIPTION_ID)
      COMMENT = 'Count of subscriptions',
    subscriptions.total_arr AS SUM(subscriptions.arr_value)
      COMMENT = 'Total ARR from subscriptions',
    subscriptions.total_users AS SUM(subscriptions.user_count)
      COMMENT = 'Total licensed users',
    subscriptions.avg_discount AS AVG(subscriptions.discount_pct)
      COMMENT = 'Average discount percentage',
    
    opportunities.total_opportunities AS COUNT(OPPORTUNITY_ID)
      COMMENT = 'Count of opportunities',
    opportunities.total_pipeline AS SUM(opportunities.deal_value)
      COMMENT = 'Total opportunity value',
    opportunities.won_deals AS COUNT_IF(STAGE = 'Closed Won')
      COMMENT = 'Count of won opportunities',
    opportunities.lost_deals AS COUNT_IF(STAGE = 'Closed Lost')
      COMMENT = 'Count of lost opportunities',
    opportunities.avg_deal_size AS AVG(opportunities.deal_value)
      COMMENT = 'Average opportunity amount'
  )

  COMMENT = 'Customer 360 semantic view for identity management SaaS business. Enables natural language queries about accounts, subscriptions, opportunities, and sales performance.';

-- Verify semantic view
SHOW SEMANTIC VIEWS IN SCHEMA PROD.FINAL;
DESCRIBE SEMANTIC VIEW PROD.FINAL.CUSTOMER_360_SEMANTIC_VIEW;
