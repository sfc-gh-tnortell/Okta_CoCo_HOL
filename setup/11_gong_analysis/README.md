# Gong Transcript Analysis: Step-by-Step Guide for Snowsight

## Overview
This guide sets up Cortex Search directly on staged Gong transcripts (no content table), then stores only sentiment metadata.

---

## Step 1: Upload Transcripts to a Stage

```sql
-- Create internal stage for Gong transcripts
CREATE OR REPLACE STAGE PROD.RAW.GONG_TRANSCRIPTS_STAGE
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');
```

**Upload files via Snowsight:**
1. Navigate to **Data → Databases → PROD → RAW → Stages**
2. Click on `GONG_TRANSCRIPTS_STAGE`
3. Click **+ Files** → Select all `.txt` files from `gong_transcripts/`
4. Click **Upload**

```sql
-- Refresh directory after upload
ALTER STAGE PROD.RAW.GONG_TRANSCRIPTS_STAGE REFRESH;

-- Verify files
SELECT * FROM DIRECTORY(@PROD.RAW.GONG_TRANSCRIPTS_STAGE) LIMIT 10;
```

📖 [Stage Documentation](https://docs.snowflake.com/en/user-guide/data-load-local-file-system-create-stage)

---

## Step 2: Create Source Table for Cortex Search

Cortex Search requires a regular table (not a directory table). Create a source table with parsed content:

```sql
-- Step 2a: Create a file format for text files
CREATE OR REPLACE FILE FORMAT PROD.RAW.TEXT_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = NONE
    RECORD_DELIMITER = NONE;
```

```sql
-- Step 2b: Create the source table
CREATE OR REPLACE TABLE PROD.RAW.GONG_TRANSCRIPT_SOURCE AS
SELECT 
    METADATA$FILENAME AS FILE_NAME,
    REPLACE(SPLIT_PART(METADATA$FILENAME, '_call_', 1), '_', ' ') AS ACCOUNT_NAME,
    TRY_TO_DATE(
        REGEXP_SUBSTR(METADATA$FILENAME, '\d{4}-\d{2}-\d{2}'), 
        'YYYY-MM-DD'
    ) AS CALL_DATE,
    $1::VARCHAR AS CONTENT
FROM @PROD.RAW.GONG_TRANSCRIPTS_STAGE
    (FILE_FORMAT => 'PROD.RAW.TEXT_FORMAT', PATTERN => '.*\.txt');
```

> **Note:** This table is the source for the Cortex Search Service. Do NOT drop it - the search service will refresh from this table when new transcripts are added.

---

## Step 3: Create Cortex Search Service

Create the search service on the source table:

```sql
CREATE OR REPLACE CORTEX SEARCH SERVICE PROD.RAW.GONG_SEARCH_SERVICE
    ON CONTENT
    ATTRIBUTES FILE_NAME, ACCOUNT_NAME, CALL_DATE
    WAREHOUSE = COMPUTE_WH
    TARGET_LAG = '1 hour'
    AS (
        SELECT 
            FILE_NAME,
            ACCOUNT_NAME,
            CALL_DATE,
            CONTENT
        FROM PROD.RAW.GONG_TRANSCRIPT_SOURCE
    );
```

📖 [Cortex Search Documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview)

**To add to Customer 360 Agent:**
```yaml
tools:
  - tool_spec:
      type: cortex_search
      name: gong_transcript_search
      spec:
        service_name: PROD.RAW.GONG_SEARCH_SERVICE
        max_results: 5
        filter_columns:
          - ACCOUNT_NAME
          - CALL_DATE
```

---

## Step 4: Create Sentiment Table (Metadata Only)

Extract sentiment from the source table, storing only metadata (no content):

```sql
CREATE OR REPLACE TABLE PROD.RAW.GONG_CALL_SENTIMENT AS
SELECT
    FILE_NAME,
    ACCOUNT_NAME,
    CALL_DATE,
    REPLACE(SPLIT_PART(FILE_NAME, '_call_', 2), '.txt', '')::INT AS CALL_NUMBER,
    AI_SENTIMENT(CONTENT):categories[0]:sentiment::VARCHAR AS SENTIMENT_CATEGORY
FROM PROD.RAW.GONG_TRANSCRIPT_SOURCE;
```

📖 [SENTIMENT Function](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions#sentiment)

> **Important:** Keep the source table - the Cortex Search Service refreshes from it when new transcripts are added.

---

## Step 5: Create Account-Level Sentiment Summary

```sql
CREATE OR REPLACE TABLE PROD.FINAL.ACCOUNT_CALL_SENTIMENT AS
SELECT
    ACCOUNT_NAME,
    COUNT(*) AS TOTAL_CALLS,
    SUM(CASE WHEN SENTIMENT_CATEGORY = 'positive' THEN 1 ELSE 0 END) AS POSITIVE_CALLS,
    SUM(CASE WHEN SENTIMENT_CATEGORY = 'negative' THEN 1 ELSE 0 END) AS NEGATIVE_CALLS,
    SUM(CASE WHEN SENTIMENT_CATEGORY = 'neutral' THEN 1 ELSE 0 END) AS NEUTRAL_CALLS,
    ROUND(SUM(CASE WHEN SENTIMENT_CATEGORY = 'positive' THEN 1 
                   WHEN SENTIMENT_CATEGORY = 'negative' THEN -1 
                   ELSE 0 END)::FLOAT / COUNT(*), 3) AS SENTIMENT_SCORE,
    MAX(CALL_DATE) AS LAST_CALL_DATE
FROM PROD.RAW.GONG_CALL_SENTIMENT
GROUP BY ACCOUNT_NAME;
```

---

## Step 6: Create Composite Health Score

```sql
CREATE OR REPLACE VIEW PROD.FINAL.ACCOUNT_HEALTH_SCORE AS
WITH account_products AS (
    SELECT
        a.ACCOUNT_NAME,
        COUNT(DISTINCT s.PRODUCT_ID) AS PRODUCTS_OWNED,
        14 AS TOTAL_PRODUCTS,
        COUNT(DISTINCT s.PRODUCT_ID) / 14.0 AS PRODUCT_COVERAGE
    FROM PROD.FINAL.ACCOUNT_DAILY a
    LEFT JOIN PROD.FINAL.SUBSCRIPTION_DAILY s ON a.ACCOUNT_ID = s.ACCOUNT_ID
    GROUP BY a.ACCOUNT_NAME
),
industry_benchmark AS (
    SELECT
        a.INDUSTRY,
        AVG(ap.PRODUCT_COVERAGE) AS AVG_INDUSTRY_COVERAGE
    FROM PROD.FINAL.ACCOUNT_DAILY a
    JOIN account_products ap ON a.ACCOUNT_NAME = ap.ACCOUNT_NAME
    GROUP BY a.INDUSTRY
)
SELECT
    a.ACCOUNT_NAME,
    a.INDUSTRY,
    
    -- Sentiment Score (0-100, weighted 50%)
    COALESCE(ROUND((s.SENTIMENT_SCORE + 1) * 50, 1), 50) AS SENTIMENT_SCORE_NORMALIZED,
    
    -- Product Whitespace Score (0-100, weighted 20%)
    -- Boosted: multiply by 3 to give credit for having any products
    LEAST(ROUND(p.PRODUCT_COVERAGE * 300, 1), 100) AS PRODUCT_COVERAGE_SCORE,
    
    -- Peer Comparison Score (0-100, weighted 30%)
    -- Boosted: accounts at or above average get 70-100, below average get 40-70
    ROUND(CASE 
        WHEN p.PRODUCT_COVERAGE >= ib.AVG_INDUSTRY_COVERAGE THEN 
            70 + (LEAST((p.PRODUCT_COVERAGE - ib.AVG_INDUSTRY_COVERAGE) / NULLIF(ib.AVG_INDUSTRY_COVERAGE, 0), 1) * 30)
        ELSE 
            40 + (30 * (p.PRODUCT_COVERAGE / NULLIF(ib.AVG_INDUSTRY_COVERAGE, 0)))
    END, 1) AS PEER_COMPARISON_SCORE,
    
    -- Composite Health Score (rebalanced weights)
    ROUND(
        (COALESCE((s.SENTIMENT_SCORE + 1) * 50, 50) * 0.50) +
        (LEAST(p.PRODUCT_COVERAGE * 300, 100) * 0.20) +
        (CASE 
            WHEN p.PRODUCT_COVERAGE >= ib.AVG_INDUSTRY_COVERAGE THEN 
                70 + (LEAST((p.PRODUCT_COVERAGE - ib.AVG_INDUSTRY_COVERAGE) / NULLIF(ib.AVG_INDUSTRY_COVERAGE, 0), 1) * 30)
            ELSE 
                40 + (30 * (p.PRODUCT_COVERAGE / NULLIF(ib.AVG_INDUSTRY_COVERAGE, 0)))
        END * 0.30)
    , 1) AS COMPOSITE_HEALTH_SCORE,
    
    CASE 
        WHEN COMPOSITE_HEALTH_SCORE >= 70 THEN 'Excellent'
        WHEN COMPOSITE_HEALTH_SCORE >= 60 THEN 'Good'
        WHEN COMPOSITE_HEALTH_SCORE >= 50 THEN 'At Risk'
        ELSE 'Critical'
    END AS HEALTH_CATEGORY,
    
    s.TOTAL_CALLS,
    s.POSITIVE_CALLS,
    s.NEGATIVE_CALLS,
    p.PRODUCTS_OWNED,
    p.TOTAL_PRODUCTS
    
FROM PROD.FINAL.ACCOUNT_DAILY a
LEFT JOIN PROD.FINAL.ACCOUNT_CALL_SENTIMENT s ON a.ACCOUNT_NAME = s.ACCOUNT_NAME
LEFT JOIN account_products p ON a.ACCOUNT_NAME = p.ACCOUNT_NAME
LEFT JOIN industry_benchmark ib ON a.INDUSTRY = ib.INDUSTRY;
```

---

## Step 7: Add Health Score to Semantic View for Cortex Analyst

Update the semantic view used by the Customer 360 Agent to include the new health score data.

### 7a: Add the ACCOUNT_HEALTH_SCORE table to your semantic view YAML

Add this table definition to the `tables` section of your semantic model:

```yaml
  - name: ACCOUNT_HEALTH_SCORE
    description: Composite account health scores combining sentiment, product coverage, and peer comparison
    base_table:
      database: PROD
      schema: FINAL
      table: ACCOUNT_HEALTH_SCORE
    dimensions:
      - name: ACCOUNT_NAME
        expr: ACCOUNT_NAME
        description: Customer account name
        data_type: VARCHAR
      - name: INDUSTRY
        expr: INDUSTRY
        description: Industry classification
        data_type: VARCHAR
      - name: HEALTH_CATEGORY
        expr: HEALTH_CATEGORY
        description: Health status category (Excellent, Good, At Risk, Critical)
        data_type: VARCHAR
    measures:
      - name: SENTIMENT_SCORE
        expr: SENTIMENT_SCORE_NORMALIZED
        description: Normalized sentiment score from call transcripts (0-100)
        data_type: NUMBER
      - name: PRODUCT_COVERAGE_SCORE
        expr: PRODUCT_COVERAGE_SCORE
        description: Product whitespace score based on products owned (0-100)
        data_type: NUMBER
      - name: PEER_COMPARISON_SCORE
        expr: PEER_COMPARISON_SCORE
        description: Score comparing account to industry peers (0-100)
        data_type: NUMBER
      - name: COMPOSITE_HEALTH_SCORE
        expr: COMPOSITE_HEALTH_SCORE
        description: Weighted composite health score (50% sentiment, 20% product, 30% peer)
        data_type: NUMBER
      - name: TOTAL_CALLS
        expr: TOTAL_CALLS
        description: Total number of Gong calls analyzed
        data_type: NUMBER
      - name: POSITIVE_CALLS
        expr: POSITIVE_CALLS
        description: Number of calls with positive sentiment
        data_type: NUMBER
      - name: NEGATIVE_CALLS
        expr: NEGATIVE_CALLS
        description: Number of calls with negative sentiment
        data_type: NUMBER
      - name: PRODUCTS_OWNED
        expr: PRODUCTS_OWNED
        description: Count of distinct products owned by account
        data_type: NUMBER
```

### 7b: Recreate the Semantic View

After updating your YAML file, recreate the semantic view:

```sql
CREATE OR REPLACE SEMANTIC VIEW PROD.FINAL.CUSTOMER_360_SEMANTIC_VIEW
  FROM @PROD.RAW.SEMANTIC_STAGE/customer_360_semantic_model.yaml;
```

### 7c: Verify the Health Score Data is Accessible

Test a query through the semantic view:

```sql
-- Sample question for Cortex Analyst:
-- "Show me accounts with Critical or At Risk health scores"
-- "What is the average health score by industry?"
-- "Which accounts have negative call sentiment but high product coverage?"
```

---

## Summary Checklist

| Step | Action | Object Created | Stores Content? |
|------|--------|----------------|-----------------|
| 1 | Upload transcript files | `@PROD.RAW.GONG_TRANSCRIPTS_STAGE` | Files only |
| 2 | Create source table | `PROD.RAW.GONG_TRANSCRIPT_SOURCE` | Yes (required for search refresh) |
| 3 | Create search service | `PROD.RAW.GONG_SEARCH_SERVICE` | Indexed |
| 4 | Extract sentiment | `PROD.RAW.GONG_CALL_SENTIMENT` | **No** - metadata + score only |
| 5 | Aggregate by account | `PROD.FINAL.ACCOUNT_CALL_SENTIMENT` | No |
| 6 | Composite health score | `PROD.FINAL.ACCOUNT_HEALTH_SCORE` | No |
| 7 | Add to semantic view | `PROD.FINAL.CUSTOMER_360_SEMANTIC_VIEW` | No - YAML update |


---

## Key Design Decision

- **Transcript content** lives only in:
  1. Stage files (source of truth)
  2. Cortex Search index (for semantic search)
- **Tables store only**: file name, account, date, sentiment score
- To read actual transcript content → use Cortex Search or query the stage directly
