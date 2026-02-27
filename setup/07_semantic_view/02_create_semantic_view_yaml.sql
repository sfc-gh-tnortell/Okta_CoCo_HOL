-- ============================================================
-- Step 7: Create Semantic View from YAML (Alternative Method)
-- ============================================================
-- This is an alternative to Option A (SQL syntax)
-- Uses SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML stored procedure

CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(
  'PROD.FINAL',
  $$
name: CUSTOMER_360_SEMANTIC_VIEW
description: Customer 360 semantic view for identity management SaaS business. Enables natural language queries about accounts, subscriptions, opportunities, and sales performance.

tables:
  - name: ACCOUNTS
    synonyms:
      - customers
      - clients
    description: Customer accounts with sales team assignments and renewal information
    base_table:
      database: PROD
      schema: FINAL
      table: ACCOUNT_DAILY
    primary_key:
      columns:
        - ACCOUNT_ID
    dimensions:
      - name: ACCOUNT_NAME
        synonyms:
          - customer name
          - company name
        description: Customer company name
        expr: ACCOUNT_NAME
        data_type: VARCHAR
      - name: ACCOUNT_STATUS
        description: "Account status: Active or Churned"
        expr: ACCOUNT_STATUS
        data_type: VARCHAR
      - name: TERRITORY
        description: "Sales territory: West, Mountain, Central, East"
        expr: TERRITORY
        data_type: VARCHAR
      - name: TIMEZONE
        description: Customer timezone
        expr: TIMEZONE
        data_type: VARCHAR
      - name: INDUSTRY
        description: Customer industry vertical
        expr: INDUSTRY
        data_type: VARCHAR
      - name: HEALTHSCORE
        synonyms:
          - health
          - account health
        description: "Account health: Excellent, Healthy, Good, At Risk, Critical"
        expr: HEALTHSCORE
        data_type: VARCHAR
      - name: ACCOUNT_EXECUTIVE
        synonyms:
          - AE
          - rep
          - sales rep
        description: Assigned account executive
        expr: ACCOUNT_EXECUTIVE
        data_type: VARCHAR
      - name: RENEWAL_DATE
        description: Contract renewal date
        expr: RENEWAL_DATE
        data_type: DATE
      - name: DAYS_TO_RENEWAL
        description: Days until contract renewal
        expr: DAYS_TO_RENEWAL
        data_type: NUMBER
      - name: ACCOUNT_ID
        expr: ACCOUNT_ID
        data_type: VARCHAR
    facts:
      - name: CARR_VALUE
        description: Contracted Annual Recurring Revenue in USD
        expr: CARR_USD
        data_type: NUMBER
      - name: EMPLOYEE_COUNT
        description: Customer employee count
        expr: NUMBER_OF_EMPLOYEES
        data_type: NUMBER
      - name: REVENUE
        description: Customer annual revenue
        expr: ANNUAL_REVENUE
        data_type: NUMBER
    metrics:
      - name: TOTAL_ACCOUNTS
        description: Count of accounts
        expr: COUNT(ACCOUNT_ID)
      - name: TOTAL_CARR
        description: Total CARR across accounts
        expr: SUM(CARR_USD)
      - name: AVG_CARR
        description: Average CARR per account
        expr: AVG(CARR_USD)
      - name: ACTIVE_ACCOUNTS
        description: Count of active accounts
        expr: COUNT_IF(ACCOUNT_STATUS = 'Active')
      - name: CHURNED_ACCOUNTS
        description: Count of churned accounts
        expr: COUNT_IF(ACCOUNT_STATUS = 'Churned')
      - name: AT_RISK_ACCOUNTS
        description: Accounts with At Risk or Critical health
        expr: COUNT_IF(HEALTHSCORE IN ('At Risk', 'Critical'))

  - name: SUBSCRIPTIONS
    synonyms:
      - licenses
      - products owned
    description: Product subscriptions with pricing and discount information
    base_table:
      database: PROD
      schema: FINAL
      table: SUBSCRIPTION_DAILY
    primary_key:
      columns:
        - SUBSCRIPTION_ID
    dimensions:
      - name: PRODUCT_NAME
        synonyms:
          - product
          - service
        description: Identity product name
        expr: PRODUCT_NAME
        data_type: VARCHAR
      - name: PRODUCT_CODE
        description: "Product code (SSO, MFA, LCM, PAM, etc.)"
        expr: PRODUCT_CODE
        data_type: VARCHAR
      - name: SUBSCRIPTION_ID
        expr: SUBSCRIPTION_ID
        data_type: VARCHAR
      - name: ACCOUNT_ID
        expr: ACCOUNT_ID
        data_type: VARCHAR
      - name: PRODUCT_ID
        expr: PRODUCT_ID
        data_type: VARCHAR
    facts:
      - name: ARR_VALUE
        description: Annual recurring revenue for subscription
        expr: ARR
        data_type: NUMBER
      - name: MRR_VALUE
        description: Monthly recurring revenue for subscription
        expr: MRR
        data_type: NUMBER
      - name: USER_COUNT
        description: Number of licensed users
        expr: QUANTITY
        data_type: NUMBER
      - name: DISCOUNT_PCT
        description: Discount percentage applied
        expr: DISCOUNT
        data_type: NUMBER
    metrics:
      - name: TOTAL_SUBSCRIPTIONS
        description: Count of subscriptions
        expr: COUNT(SUBSCRIPTION_ID)
      - name: TOTAL_ARR
        description: Total ARR from subscriptions
        expr: SUM(ARR)
      - name: TOTAL_USERS
        description: Total licensed users
        expr: SUM(QUANTITY)
      - name: AVG_DISCOUNT
        description: Average discount percentage
        expr: AVG(DISCOUNT)

  - name: OPPORTUNITIES
    synonyms:
      - deals
      - sales
    description: Sales opportunities including expansion attempts and lost deals
    base_table:
      database: PROD
      schema: FINAL
      table: OPPORTUNITY_DAILY
    primary_key:
      columns:
        - OPPORTUNITY_ID
    dimensions:
      - name: STAGE
        description: "Opportunity stage: Closed Won or Closed Lost"
        expr: STAGE
        data_type: VARCHAR
      - name: LOSS_REASON
        synonyms:
          - why lost
          - reason for loss
        description: Reason for lost opportunity
        expr: LOSS_REASON
        data_type: VARCHAR
      - name: COMPETITOR
        description: Competitor that won the deal
        expr: COMPETITOR
        data_type: VARCHAR
      - name: CLOSE_DATE
        description: Date opportunity was closed
        expr: CLOSE_DATE
        data_type: DATE
      - name: OPPORTUNITY_ID
        expr: OPPORTUNITY_ID
        data_type: VARCHAR
      - name: ACCOUNT_ID
        expr: ACCOUNT_ID
        data_type: VARCHAR
      - name: PRODUCT_ID
        expr: PRODUCT_ID
        data_type: VARCHAR
    facts:
      - name: DEAL_VALUE
        description: Opportunity value in dollars
        expr: AMOUNT
        data_type: NUMBER
      - name: CLOSE_DURATION
        description: Days from creation to close
        expr: DAYS_TO_CLOSE
        data_type: NUMBER
    metrics:
      - name: TOTAL_OPPORTUNITIES
        description: Count of opportunities
        expr: COUNT(OPPORTUNITY_ID)
      - name: TOTAL_PIPELINE
        description: Total opportunity value
        expr: SUM(AMOUNT)
      - name: WON_DEALS
        description: Count of won opportunities
        expr: COUNT_IF(STAGE = 'Closed Won')
      - name: LOST_DEALS
        description: Count of lost opportunities
        expr: COUNT_IF(STAGE = 'Closed Lost')
      - name: AVG_DEAL_SIZE
        description: Average opportunity amount
        expr: AVG(AMOUNT)

  - name: PRODUCTS
    description: Identity management product catalog
    base_table:
      database: PROD
      schema: RAW
      table: SFDC_PRODUCT
    primary_key:
      columns:
        - PRODUCT_ID
    dimensions:
      - name: PRODUCT_NAME
        description: Product catalog name
        expr: PRODUCT_NAME
        data_type: VARCHAR
      - name: PRODUCT_FAMILY
        description: Product family grouping
        expr: PRODUCT_FAMILY
        data_type: VARCHAR
      - name: PRODUCT_ID
        expr: PRODUCT_ID
        data_type: VARCHAR
    facts:
      - name: LIST_PRICE_USD
        description: List price per user per month
        expr: LIST_PRICE_USD
        data_type: NUMBER

relationships:
  - name: SUBSCRIPTIONS_TO_ACCOUNTS
    left_table: SUBSCRIPTIONS
    right_table: ACCOUNTS
    relationship_columns:
      - left_column: ACCOUNT_ID
        right_column: ACCOUNT_ID
    relationship_type: many_to_one
  - name: OPPORTUNITIES_TO_ACCOUNTS
    left_table: OPPORTUNITIES
    right_table: ACCOUNTS
    relationship_columns:
      - left_column: ACCOUNT_ID
        right_column: ACCOUNT_ID
    relationship_type: many_to_one
  - name: SUBSCRIPTIONS_TO_PRODUCTS
    left_table: SUBSCRIPTIONS
    right_table: PRODUCTS
    relationship_columns:
      - left_column: PRODUCT_ID
        right_column: PRODUCT_ID
    relationship_type: many_to_one
  - name: OPPORTUNITIES_TO_PRODUCTS
    left_table: OPPORTUNITIES
    right_table: PRODUCTS
    relationship_columns:
      - left_column: PRODUCT_ID
        right_column: PRODUCT_ID
    relationship_type: many_to_one
$$
);

-- Verify the semantic view was created
SHOW SEMANTIC VIEWS IN SCHEMA PROD.FINAL;
DESCRIBE SEMANTIC VIEW PROD.FINAL.CUSTOMER_360_SEMANTIC_VIEW;
