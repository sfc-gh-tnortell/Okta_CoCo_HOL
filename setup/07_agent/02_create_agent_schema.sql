-- ============================================================
-- Step 8b: Create Agent Schema
-- ============================================================

USE ROLE SYSADMIN;
USE WAREHOUSE DEFAULT_WH;

CREATE DATABASE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE;
CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_INTELLIGENCE.AGENTS;
