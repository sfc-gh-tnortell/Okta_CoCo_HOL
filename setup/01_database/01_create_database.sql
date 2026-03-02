-- ============================================================
-- Step 1: Create Database and Schemas
-- ============================================================

USE ROLE ACCOUNTADMIN;
GRANT ALL ON WAREHOUSE DEFAULT_WH TO ROLE SYSADMIN;

USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS PROD;
CREATE SCHEMA IF NOT EXISTS PROD.RAW;
CREATE SCHEMA IF NOT EXISTS PROD.FINAL;

SHOW SCHEMAS IN DATABASE PROD;
