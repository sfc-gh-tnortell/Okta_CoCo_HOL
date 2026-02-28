-- ============================================================
-- Step 10d: Create Sentiment Analysis and Health Score
-- ============================================================
-- Extracts sentiment from transcripts and creates composite health score

-- Step 4a: Create Sentiment Table (Metadata Only)
-- Extracts sentiment from source table, storing only metadata (no content duplication)
CREATE OR REPLACE TABLE PROD.RAW.GONG_CALL_SENTIMENT AS
SELECT
    FILE_NAME,
    ACCOUNT_NAME,
    CALL_DATE,
    REPLACE(SPLIT_PART(FILE_NAME, '_call_', 2), '.txt', '')::INT AS CALL_NUMBER,
    AI_SENTIMENT(CONTENT):categories[0]:sentiment::VARCHAR AS SENTIMENT_CATEGORY
FROM PROD.RAW.GONG_TRANSCRIPT_SOURCE;

-- Step 4b: Create Account-Level Sentiment Summary
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

-- Step 4c: Create Composite Health Score View
-- Combines sentiment, product coverage, and peer comparison into a single health score
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
    LEAST(ROUND(p.PRODUCT_COVERAGE * 300, 1), 100) AS PRODUCT_COVERAGE_SCORE,
    
    -- Peer Comparison Score (0-100, weighted 30%)
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
