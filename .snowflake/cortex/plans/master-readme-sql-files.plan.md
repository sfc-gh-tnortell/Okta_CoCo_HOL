# Master README and SQL Files Plan

## Overview
Create comprehensive documentation and extract SQL into separate files for steps 11 and 13.

## Step 10: Gong Analysis SQL Files

Extract from README into:
- `01_create_stage.sql` - Create internal stage for transcripts
- `02_create_source_table.sql` - Create GONG_TRANSCRIPT_SOURCE table
- `03_create_search_service.sql` - Create Cortex Search service
- `04_create_sentiment_table.sql` - Create GONG_CALL_SENTIMENT table
- `05_create_sentiment_summary.sql` - Create ACCOUNT_CALL_SENTIMENT table
- `06_create_health_score.sql` - Create ACCOUNT_HEALTH_SCORE view
- `06_semantic_view_update.sql` - YAML snippet for semantic view update

## Step 13: Postgres Activity Logs SQL Files

Extract from README into:
- `01_create_network_rule.sql` - Network rule and policy for Postgres
- `02_create_postgres_tables.sql` - DDL for users, product_user_assignment, device_auth_logs (PostgreSQL)
- `03_enable_replication.sql` - Enable CDC replication
- `04_configure_external_access.sql` - External access integration for Openflow
- `05_verification_queries.sql` - Verification queries for Snowflake

## Master README Structure

1. Introduction and Prerequisites
2. Steps 1-10: Core Setup (reference existing README)
3. Step 10: Gong Analysis Integration
4. Step 12: Web Search
5. Step 13: Postgres Activity Logs Pipeline
6. Data Governance (brief mention)
7. Sample Questions
