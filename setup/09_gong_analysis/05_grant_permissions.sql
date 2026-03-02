-- ============================================================
-- Step 10e: Grant Permissions for Gong Analysis Objects
-- ============================================================
-- Run these grants to allow other roles to use the Gong analysis objects

USE ROLE SYSADMIN;
USE WAREHOUSE DEFAULT_WH;

-- Grant select on Gong tables
GRANT SELECT ON TABLE PROD.RAW.GONG_TRANSCRIPT_SOURCE TO ROLE PUBLIC;
GRANT SELECT ON TABLE PROD.RAW.GONG_CALL_SENTIMENT TO ROLE PUBLIC;
GRANT SELECT ON TABLE PROD.FINAL.ACCOUNT_CALL_SENTIMENT TO ROLE PUBLIC;
GRANT SELECT ON VIEW PROD.FINAL.ACCOUNT_HEALTH_SCORE TO ROLE PUBLIC;

-- Grant usage on Gong Search service
GRANT USAGE ON CORTEX SEARCH SERVICE PROD.RAW.GONG_SEARCH_SERVICE TO ROLE PUBLIC;
