-- ============================================================
-- Step 10e: Grant Permissions for Gong Analysis Objects
-- ============================================================
-- Run these grants to allow other roles to use the Gong analysis objects

-- Grant select on Gong tables
GRANT SELECT ON TABLE PROD.RAW.GONG_TRANSCRIPT_SOURCE TO ROLE PUBLIC;
GRANT SELECT ON TABLE PROD.RAW.GONG_CALL_SENTIMENT TO ROLE PUBLIC;
GRANT SELECT ON TABLE PROD.FINAL.ACCOUNT_CALL_SENTIMENT TO ROLE PUBLIC;
GRANT SELECT ON VIEW PROD.FINAL.ACCOUNT_HEALTH_SCORE TO ROLE PUBLIC;

-- Grant usage on Transcript Search service
GRANT USAGE ON CORTEX SEARCH SERVICE PROD.FINAL.TRANSCRIPT_SEARCH TO ROLE PUBLIC;
