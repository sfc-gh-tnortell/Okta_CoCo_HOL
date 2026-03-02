-- ============================================================
-- Step 3c: Fix Timezone Distribution (Optional)
-- ============================================================
-- Run only if timezone distribution is uneven

USE ROLE SYSADMIN;
USE WAREHOUSE DEFAULT_WH;

UPDATE PROD.RAW.SFDC_ACCOUNT
SET TIMEZONE = CASE MOD(ABS(HASH(ACCOUNT_ID)), 4)
    WHEN 0 THEN 'Pacific'
    WHEN 1 THEN 'Mountain'
    WHEN 2 THEN 'Central'
    WHEN 3 THEN 'Eastern'
END,
TERRITORY = CASE MOD(ABS(HASH(ACCOUNT_ID)), 4)
    WHEN 0 THEN 'West'
    WHEN 1 THEN 'Mountain'
    WHEN 2 THEN 'Central'
    WHEN 3 THEN 'East'
END;

SELECT TIMEZONE, COUNT(*) FROM PROD.RAW.SFDC_ACCOUNT GROUP BY TIMEZONE ORDER BY TIMEZONE;
