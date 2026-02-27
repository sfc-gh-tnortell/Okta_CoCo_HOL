-- ============================================================
-- Step 3e: Generate Subscriptions (2-6 per Contract)
-- ============================================================

INSERT INTO PROD.RAW.SFDC_SUBSCRIPTION_CPQ
WITH contract_products AS (
    SELECT 
        c.CONTRACT_ID,
        c.CONTRACT_NUMBER,
        c.ACCOUNT_ID,
        c.START_DATE,
        c.END_DATE,
        p.PRODUCT_ID,
        p.PRODUCT_NAME,
        p.LIST_PRICE_USD,
        ROW_NUMBER() OVER (PARTITION BY c.CONTRACT_ID ORDER BY RANDOM()) AS product_rank
    FROM PROD.RAW.SFDC_CONTRACT c
    CROSS JOIN PROD.RAW.SFDC_PRODUCT p
),
filtered_products AS (
    SELECT * FROM contract_products
    WHERE product_rank <= 2 + MOD(ABS(HASH(CONTRACT_ID)), 5)
)
SELECT 
    'SUB' || LPAD(ROW_NUMBER() OVER (ORDER BY CONTRACT_ID, PRODUCT_ID)::VARCHAR, 7, '0'),
    PRODUCT_NAME || ' - ' || CONTRACT_NUMBER,
    ACCOUNT_ID,
    CONTRACT_ID,
    CONTRACT_NUMBER,
    PRODUCT_ID,
    PRODUCT_NAME,
    'USD',
    START_DATE,
    END_DATE,
    ROUND(50 + MOD(ABS(HASH(CONTRACT_ID || PRODUCT_ID)), 4950), 0) AS quantity,
    LIST_PRICE_USD,
    ROUND(MOD(ABS(HASH(CONTRACT_ID || PRODUCT_ID || 'disc')), 31), 2) AS discount,
    ROUND(LIST_PRICE_USD * (1 - MOD(ABS(HASH(CONTRACT_ID || PRODUCT_ID || 'disc')), 31) / 100), 2) AS customer_price,
    ROUND(LIST_PRICE_USD * (1 - MOD(ABS(HASH(CONTRACT_ID || PRODUCT_ID || 'disc')), 31) / 100), 2) AS net_price,
    ROUND((50 + MOD(ABS(HASH(CONTRACT_ID || PRODUCT_ID)), 4950)) * LIST_PRICE_USD * (1 - MOD(ABS(HASH(CONTRACT_ID || PRODUCT_ID || 'disc')), 31) / 100) * 12, 2) AS arr,
    ROUND((50 + MOD(ABS(HASH(CONTRACT_ID || PRODUCT_ID)), 4950)) * LIST_PRICE_USD * (1 - MOD(ABS(HASH(CONTRACT_ID || PRODUCT_ID || 'disc')), 31) / 100), 2) AS mrr,
    ROUND((50 + MOD(ABS(HASH(CONTRACT_ID || PRODUCT_ID)), 4950)) * LIST_PRICE_USD * (1 - MOD(ABS(HASH(CONTRACT_ID || PRODUCT_ID || 'disc')), 31) / 100) * 12, 2) AS tcv,
    CURRENT_TIMESTAMP()
FROM filtered_products;

SELECT COUNT(*) AS subscription_count, COUNT(DISTINCT CONTRACT_ID) AS contracts FROM PROD.RAW.SFDC_SUBSCRIPTION_CPQ;
