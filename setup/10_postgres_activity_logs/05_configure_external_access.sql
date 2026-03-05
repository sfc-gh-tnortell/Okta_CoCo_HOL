-- Step 4: Configure External Access for Openflow
-- Openflow runs in SPCS and needs network access to reach your Postgres instance

USE ROLE ACCOUNTADMIN;

-- Step 1: Create Role and Database
CREATE ROLE IF NOT EXISTS Postgres_HOL_ROLE;

CREATE DATABASE IF NOT EXISTS Okta_PGCDC_DB;

CREATE WAREHOUSE IF NOT EXISTS Okta_PGCDC_WH
  WAREHOUSE_SIZE = MEDIUM
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE;

-- Grant privileges to runtime role
GRANT OWNERSHIP ON DATABASE Okta_PGCDC_DB TO ROLE Postgres_HOL_ROLE;
GRANT OWNERSHIP ON SCHEMA Okta_PGCDC_DB.PUBLIC TO ROLE Postgres_HOL_ROLE;
GRANT USAGE ON WAREHOUSE Okta_PGCDC_WH TO ROLE Postgres_HOL_ROLE;

-- Grant runtime role to OpenFlow admin
GRANT ROLE Postgres_HOL_ROLE TO ROLE OPENFLOWADMIN;

-- Step 2: Create Schema and Network Rules
USE ROLE Postgres_HOL_ROLE;
USE DATABASE Okta_PGCDC_DB;

CREATE SCHEMA IF NOT EXISTS Okta_PGCDC_DB.NETWORKS;

-- Step 3: Create Network Rules
-- IMPORTANT: Replace YOUR-POSTGRES-HOST with your PostgreSQL endpoint
-- Examples:
-- - GCP Cloud SQL:   '34.123.45.67:5432'
-- - AWS RDS:         'mydb.abc123.us-east-1.rds.amazonaws.com:5432'
-- - Azure Database:  'myserver.postgres.database.azure.com:5432'

CREATE OR REPLACE NETWORK RULE Okta_PGCDC_DB.NETWORKS.postgres_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('YOUR-POSTGRES-HOST:5432');

-- Step 4: Create External Access Integration
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION okta_pgcdc_access
  ALLOWED_NETWORK_RULES = (
    Okta_PGCDC_DB.NETWORKS.postgres_network_rule
  )
  ENABLED = TRUE
  COMMENT = 'OpenFlow SPCS runtime access for Okta CDC';

-- Grant usage to runtime role
GRANT USAGE ON INTEGRATION okta_pgcdc_access TO ROLE Postgres_HOL_ROLE;
