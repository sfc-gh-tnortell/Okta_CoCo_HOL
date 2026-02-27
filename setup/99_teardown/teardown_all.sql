-- ============================================================
-- TEARDOWN: Remove All HOL Objects
-- ============================================================
-- This script removes ALL objects created during the Okta Customer 360
-- Hands-On Lab. Run this to completely clean up your environment.
--
-- WARNING: This will permanently delete all data and objects!
-- Review each section before executing.
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- ============================================================
-- STEP 1: Drop Agent
-- ============================================================
DROP AGENT IF EXISTS SNOWFLAKE_INTELLIGENCE.AGENTS.CUSTOMER_360_AGENT;

-- ============================================================
-- STEP 2: Drop Cortex Search Services
-- ============================================================
DROP CORTEX SEARCH SERVICE IF EXISTS PROD.FINAL.CONTRACT_SEARCH;
DROP CORTEX SEARCH SERVICE IF EXISTS PROD.FINAL.TRANSCRIPT_SEARCH;
DROP CORTEX SEARCH SERVICE IF EXISTS PROD.RAW.GONG_SEARCH_SERVICE;

-- ============================================================
-- STEP 3: Drop Semantic View
-- ============================================================
DROP SEMANTIC VIEW IF EXISTS PROD.FINAL.CUSTOMER_360_SEMANTIC_VIEW;

-- ============================================================
-- STEP 4: Drop the PROD Database (and all contents)
-- ============================================================
-- This removes ALL schemas, tables, views, stages, etc. in PROD
DROP DATABASE IF EXISTS PROD;

-- ============================================================
-- STEP 5: Drop SNOWFLAKE_INTELLIGENCE Database
-- ============================================================
-- Only drop if you created this specifically for the HOL
-- Comment out if you have other agents in this database
DROP DATABASE IF EXISTS SNOWFLAKE_INTELLIGENCE;

-- ============================================================
-- STEP 6: Drop Postgres CDC Objects (Step 12)
-- ============================================================
-- Drop External Access Integration
DROP INTEGRATION IF EXISTS okta_pgcdc_access;

-- Drop the CDC Database (includes all synced tables)
DROP DATABASE IF EXISTS Okta_PGCDC_DB;

-- Drop the CDC Warehouse
DROP WAREHOUSE IF EXISTS Okta_PGCDC_WH;

-- Drop the CDC Role
DROP ROLE IF EXISTS Postgres_HOL_ROLE;

-- ============================================================
-- STEP 7: Drop Network Policies and Rules
-- ============================================================
-- Note: Network policy must be dropped before network rules
DROP NETWORK POLICY IF EXISTS POSTGRES_ACCESS_POLICY;

-- Network rules in PROD.NETWORK were dropped with PROD database
-- If you created any account-level network rules, drop them here:
-- DROP NETWORK RULE IF EXISTS <rule_name>;

-- ============================================================
-- STEP 8: Revoke Grants (if not using CASCADE)
-- ============================================================
-- Most grants are automatically revoked when objects are dropped.
-- If you granted to specific roles beyond PUBLIC, revoke here:
-- REVOKE USAGE ON DATABASE PROD FROM ROLE <role_name>;

-- ============================================================
-- STEP 9: Clean up Snowflake Postgres Instance (Manual)
-- ============================================================
-- If you created a Snowflake Postgres instance, delete it via UI:
-- 1. Navigate to Data → Databases
-- 2. Find your Postgres database (e.g., okta_activity_logs)
-- 3. Click ••• → Delete
-- Note: This cannot be done via SQL for managed Postgres

-- ============================================================
-- STEP 10: Clean up Openflow Runtime (Manual)
-- ============================================================
-- If you created an Openflow runtime and connectors:
-- 1. Navigate to Data → Ingestion → Openflow → Runtimes
-- 2. Stop and delete any connectors
-- 3. Delete the runtime

-- ============================================================
-- STEP 11: Clean up Cortex Analyst (Manual)
-- ============================================================
-- If you created a Cortex Analyst in the UI:
-- 1. Navigate to AI & ML → Cortex Analyst
-- 2. Find and delete "Customer 360 Analyst"

-- ============================================================
-- VERIFICATION: Confirm Objects Are Removed
-- ============================================================
-- Run these to verify cleanup:

SHOW DATABASES LIKE 'PROD';
SHOW DATABASES LIKE 'SNOWFLAKE_INTELLIGENCE';
SHOW DATABASES LIKE 'Okta_PGCDC_DB';
SHOW WAREHOUSES LIKE 'Okta_PGCDC_WH';
SHOW ROLES LIKE 'Postgres_HOL_ROLE';
SHOW NETWORK POLICIES LIKE 'POSTGRES_ACCESS_POLICY';
SHOW INTEGRATIONS LIKE 'okta_pgcdc_access';

-- If all queries return empty results, teardown is complete!

-- ============================================================
-- END OF TEARDOWN
-- ============================================================
