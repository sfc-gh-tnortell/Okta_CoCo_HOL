-- Step 1d & 1e: Enable Replication for CDC
-- Run these statements in your Snowflake Postgres instance

-- Enable replication for the admin user (required for Openflow CDC)
ALTER USER snowflake_admin WITH REPLICATION;

-- Create publication for all tables in public schema (required for Openflow CDC)
-- NOTE: Run this AFTER creating the tables in Step 2
CREATE PUBLICATION openflow_publication FOR TABLES IN SCHEMA public;

-- Verify the publication
SELECT * FROM pg_publication;

-- Verify replication role
SELECT rolname, rolreplication FROM pg_roles WHERE rolname = 'snowflake_admin';
