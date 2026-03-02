# Okta Customer 360 Demo - Complete Setup Guide

This comprehensive guide covers the full setup of the Okta Customer 360 demo environment for Snowflake Intelligence, including core data setup, Gong transcript analysis, web search integration, and real-time activity log streaming via Snowflake Postgres and Openflow CDC.

---

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Part 1: Core Setup (Steps 1-9)](#part-1-core-setup-steps-1-9)
4. [Part 2: Gong Transcript Analysis (Step 10)](#part-2-gong-transcript-analysis-step-10)
5. [Part 3: Postgres Activity Logs Pipeline (Step 11)](#part-3-postgres-activity-logs-pipeline-step-11)
6. [Step 12: Choose Your Own Adventure](#step-12-choose-your-own-adventure-)
7. [Sample Questions](#sample-questions)
8. [Data Summary](#data-summary)
9. [Troubleshooting](#troubleshooting)
10. [Teardown / Cleanup](#teardown--cleanup)

---

## Overview

### What You'll Build

- **250 Fortune 500 customer accounts** with realistic CRM data
- **250 contracts** with product subscriptions and pricing
- **~970 subscriptions** across 14 identity products (SSO, MFA, PAM, etc.)
- **500 opportunities** including failed expansion attempts with loss reasons
- **~150 Gong call transcripts** with business insights (fiscal planning, layoffs, tech changes)
- **Cortex Search Services** for semantic search over contracts and transcripts
- **Semantic View** for natural language SQL queries
- **Cortex Agent** for Snowflake Intelligence
- **Real-time activity log streaming** via Snowflake Postgres and Openflow CDC

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Snowflake Intelligence                           │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    Customer 360 Agent                            │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌────────────────────────┐ │   │
│  │  │Cortex Analyst│  │Cortex Search │  │     Web Search         │ │   │
│  │  │(Semantic View)│  │ (Contracts)  │  │(Public Company Info)  │ │   │
│  │  └──────────────┘  └──────────────┘  └────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                │                                        │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                     PROD Database                                │   │
│  │  ┌────────────────┐  ┌──────────────┐  ┌─────────────────────┐  │   │
│  │  │ RAW Schema     │  │FINAL Schema  │  │ AGENT Schema        │  │   │
│  │  │ - SFDC_*       │  │ - *_DAILY DT │  │ - Agent Definition  │  │   │
│  │  │ - GONG_*       │  │ - Health     │  │                     │  │   │
│  │  └────────────────┘  └──────────────┘  └─────────────────────┘  │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                   Okta_PGCDC_DB (CDC Pipeline)                   │   │
│  │  ┌────────────────────────────────────────────────────────────┐ │   │
│  │  │ "public" Schema (from Postgres CDC)                        │ │   │
│  │  │  - "users", "product_user_assignment", "device_auth_logs"  │ │   │
│  │  └────────────────────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                                   ▲
                                   │ Openflow CDC
                                   │
┌─────────────────────────────────────────────────────────────────────────┐
│                     Snowflake Postgres Instance                         │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ public schema: users, product_user_assignment, device_auth_logs  │  │
│  └──────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Prerequisites

- Snowflake account with Cortex features enabled
- ACCOUNTADMIN or SYSADMIN role
- COMPUTE_WH warehouse (or modify scripts to use your warehouse)
- Python 3.x with packages (for PDF/transcript generation):
  ```bash
  pip install snowflake-connector-python reportlab
  ```
- For Step 11: Existing Openflow deployment

---

## Part 1: Core Setup (Steps 1-9)

### Folder Structure

```
setup/
├── 01_database/          # Create database and schemas
├── 02_raw_tables/        # Create source tables (SFDC_*, SALES_TEAM)
├── 03_data_generation/   # Generate synthetic data + parse contracts
├── 05_pdf_contracts/     # Create enriched content + search service
├── 06_final_schema/      # Create dynamic tables
├── 07_semantic_view/     # Create semantic view for Cortex Analyst
├── 08_agent/             # Create Cortex Agent
├── 09_grants/            # Grant permissions
```

---

### Step 1: Database Setup

📁 **File**: `setup/01_database/01_create_database.sql`

Creates the PROD database with RAW and FINAL schemas:

```sql
USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS PROD;
CREATE SCHEMA IF NOT EXISTS PROD.RAW;
CREATE SCHEMA IF NOT EXISTS PROD.FINAL;

-- Verify
SHOW SCHEMAS IN DATABASE PROD;
```

---

### Step 2: Create Raw Tables

📁 **File**: `setup/02_raw_tables/create_raw_tables.sql`

Creates source tables for CRM data:
- `SFDC_ACCOUNT` - Customer accounts
- `SFDC_PRODUCT` - Identity products
- `SFDC_CONTRACT` - Customer contracts
- `SFDC_SUBSCRIPTION` - Product subscriptions
- `SFDC_OPPORTUNITY` - Sales opportunities
- `SALES_TEAM` - Sales team assignments

**Verify:**
```sql
SHOW TABLES LIKE 'SFDC%' IN SCHEMA PROD.RAW;
```

---

### Step 3: Generate Data

Run these scripts in order:

📁 **Files**:
```
setup/03_data_generation/01_insert_products.sql
setup/03_data_generation/02_insert_accounts.sql
setup/03_data_generation/03_fix_timezone_distribution.sql
setup/03_data_generation/04_create_contract_stage.sql
```

**Upload PDF contracts:**
1. Navigate to **Data → Databases → PROD → RAW → Stages**
2. Click on `CONTRACTS_STAGE`
3. Click **+ Files** → Select all PDF files from `unstructured_data/contracts_pdf/`
4. Click **Upload**

Then continue with:
```
setup/03_data_generation/05_insert_contracts.sql
setup/03_data_generation/06_insert_subscriptions.sql
setup/03_data_generation/07_insert_opportunities.sql
setup/03_data_generation/08_insert_sales_teams.sql
```

---

### Step 5: PDF Contracts Pipeline

📁 **Files**:
```
setup/05_pdf_contracts/01_create_content_table.sql
setup/05_pdf_contracts/02_create_search_service.sql
```

Creates enriched contract content and Cortex Search service for semantic search over contract documents.

---

### Step 6: Final Schema (Dynamic Tables)

📁 **Files**:
```
setup/06_final_schema/01_create_account_daily_dt.sql
setup/06_final_schema/02_create_subscription_daily_dt.sql
setup/06_final_schema/03_create_opportunity_daily_dt.sql
```

Creates dynamic tables that automatically refresh when source data changes:
- `ACCOUNT_DAILY` - Enriched account data with calculated fields
- `SUBSCRIPTION_DAILY` - Subscription details with ARR calculations
- `OPPORTUNITY_DAILY` - Opportunity data with status tracking

---

### Step 7: Semantic View

📁 **File (Option A - SQL)**: `setup/07_semantic_view/01_create_semantic_view.sql`

📁 **File (Option B - YAML)**: `setup/07_semantic_view/02_create_semantic_view_yaml.sql`

Creates the semantic view for Cortex Analyst natural language queries:

```sql
CREATE OR REPLACE SEMANTIC VIEW PROD.FINAL.CUSTOMER_360_SEMANTIC_VIEW
  TABLES (
    accounts AS PROD.FINAL.ACCOUNT_DAILY PRIMARY KEY (ACCOUNT_ID)
      WITH SYNONYMS ('customers', 'clients', 'companies'),
    subscriptions AS PROD.FINAL.SUBSCRIPTION_DAILY PRIMARY KEY (SUBSCRIPTION_ID),
    opportunities AS PROD.FINAL.OPPORTUNITY_DAILY PRIMARY KEY (OPPORTUNITY_ID)
    -- ... additional configuration
  )
  RELATIONSHIPS (...)
  FACTS (...)
  DIMENSIONS (...)
  METRICS (...);
```

---

### Step 8: Cortex Agent (with Web Search)

#### 8a: Enable Web Search (Account Level)

Web search must be enabled at the account level **BEFORE** creating the agent:

1. Sign in to **Snowsight**
2. Navigate to **AI & ML → Agents → Settings** (gear icon)
3. Toggle **Web search** to enable

> **Note**: This is a one-time account-level setting requiring ACCOUNTADMIN.

#### 8b: Create Agent Schema

📁 **File**: `setup/08_agent/02_create_agent_schema.sql`

```sql
USE ROLE SYSADMIN;
CREATE DATABASE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE;
CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_INTELLIGENCE.AGENTS;
```

#### 8c: Create Agent

📁 **File**: `setup/08_agent/03_create_agent.sql`

```sql
CREATE OR REPLACE AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.CUSTOMER_360_AGENT
COMMENT = 'Customer 360 Assistant for account health, contracts, and business insights'
PROFILE = '{"display_name": "Customer 360 Assistant", "color": "blue"}'
FROM SPECIFICATION
$$
models:
  orchestration: claude-4-sonnet
tools:
  - tool_spec:
      type: cortex_analyst_text_to_sql
      name: Analyst
  - tool_spec:
      type: cortex_search
      name: ContractSearch
  - tool_spec:
      type: web_search
      name: WebSearch
tool_resources:
  Analyst:
    semantic_view: PROD.FINAL.CUSTOMER_360_SEMANTIC_VIEW
  ContractSearch:
    name: PROD.FINAL.CONTRACT_SEARCH
    max_results: "10"
$$;
```

The agent includes:
- **Analyst**: Cortex Analyst with semantic view for structured data queries
- **ContractSearch**: Cortex Search over contract documents
- **WebSearch**: Web search for public company information

> **Note**: TranscriptSearch is added later in Step 10 after creating the Gong analysis pipeline.

**Verify in Snowflake Intelligence:**
1. Navigate to **AI & ML → Snowflake Intelligence**
2. The `CUSTOMER_360_AGENT` should appear in the agent list
3. Click to open and test with: *"Which accounts are at risk?"*

---

### Step 9: Permissions

📁 **File**: `setup/09_grants/01_grant_permissions.sql`

```sql
-- Grant access to semantic view
GRANT SELECT ON SEMANTIC VIEW PROD.FINAL.CUSTOMER_360_SEMANTIC_VIEW TO ROLE PUBLIC;

-- Grant access to agent
GRANT USAGE ON AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.CUSTOMER_360_AGENT TO ROLE PUBLIC;
```

---

## Part 2: Gong Transcript Analysis (Step 10)

This step creates a semantic search service over Gong call transcripts and builds a composite account health score.

### Folder Structure

```
setup/10_gong_analysis/
├── 01_create_stage.sql              # Create stage for transcripts
├── 02_create_source_table.sql       # Parse transcripts into table
├── 03_create_search_service.sql     # Cortex Search on transcripts
├── 04_create_sentiment_analysis.sql # Sentiment extraction + health score
├── 05_grant_permissions.sql         # Grant access to Gong objects
```

---

### Step 10a: Create Transcript Stage

📁 **File**: `setup/10_gong_analysis/01_create_stage.sql`

```sql
USE ROLE SYSADMIN;
USE DATABASE PROD;
USE SCHEMA RAW;

-- Create stage for Gong transcripts
CREATE OR REPLACE STAGE PROD.RAW.GONG_TRANSCRIPTS_STAGE
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

-- Verify stage created
SHOW STAGES LIKE 'GONG_TRANSCRIPTS_STAGE' IN SCHEMA PROD.RAW;
```

**Upload transcript files via Snowsight:**
1. Navigate to **Data → Databases → PROD → RAW → Stages**
2. Click on `GONG_TRANSCRIPTS_STAGE`
3. Click **+ Files** → Select all `.txt` files from `unstructured_data/gong_transcripts/`
4. Click **Upload**

Then refresh the directory:
```sql
ALTER STAGE PROD.RAW.GONG_TRANSCRIPTS_STAGE REFRESH;
SELECT * FROM DIRECTORY(@PROD.RAW.GONG_TRANSCRIPTS_STAGE) LIMIT 10;
```

---

### Step 10b: Create Source Table

📁 **File**: `setup/10_gong_analysis/02_create_source_table.sql`

Parses transcript filenames to extract account name and call date:

```sql
-- Filename format: AccountName_YYYY-MM-DD_call_N.txt
CREATE OR REPLACE TABLE PROD.RAW.GONG_TRANSCRIPT_SOURCE AS
SELECT 
    METADATA$FILENAME AS FILE_NAME,
    REPLACE(
        REPLACE(
            REGEXP_REPLACE(METADATA$FILENAME, '_\\d{4}-\\d{2}-\\d{2}_call_\\d+\\.txt$', ''),
            '_', ' '
        ),
        'and', '&'
    ) AS ACCOUNT_NAME,
    TRY_TO_DATE(
        REGEXP_SUBSTR(METADATA$FILENAME, '\\d{4}-\\d{2}-\\d{2}'),
        'YYYY-MM-DD'
    ) AS CALL_DATE,
    $1::VARCHAR AS CONTENT
FROM @PROD.RAW.GONG_TRANSCRIPTS_STAGE
    (FILE_FORMAT => 'PROD.RAW.TEXT_FORMAT', PATTERN => '.*\.txt');
```

---

### Step 10c: Create Search Service

📁 **File**: `setup/10_gong_analysis/03_create_search_service.sql`

Creates a Cortex Search service for semantic search over transcript content:

```sql
CREATE OR REPLACE CORTEX SEARCH SERVICE PROD.FINAL.TRANSCRIPT_SEARCH
ON CONTENT
WAREHOUSE = COMPUTE_WH
TARGET_LAG = '1 hour'
AS (
    SELECT 
        FILE_NAME,
        ACCOUNT_NAME,
        CALL_DATE,
        CONTENT
    FROM PROD.RAW.GONG_TRANSCRIPT_SOURCE
);
```

---

### Step 10d: Create Sentiment Analysis & Health Score

📁 **File**: `setup/10_gong_analysis/04_create_sentiment_analysis.sql`

Uses `SNOWFLAKE.CORTEX.SENTIMENT` to analyze call transcripts and create a composite health score:

```sql
-- Per-call sentiment analysis
CREATE OR REPLACE TABLE PROD.RAW.GONG_CALL_SENTIMENT AS
SELECT 
    FILE_NAME,
    ACCOUNT_NAME,
    CALL_DATE,
    SNOWFLAKE.CORTEX.SENTIMENT(CONTENT) AS SENTIMENT_SCORE,
    CASE 
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(CONTENT) >= 0.3 THEN 'Positive'
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(CONTENT) <= -0.3 THEN 'Negative'
        ELSE 'Neutral'
    END AS SENTIMENT_CATEGORY
FROM PROD.RAW.GONG_TRANSCRIPT_SOURCE;

-- Composite health score view combining sentiment, products, and peer comparison
CREATE OR REPLACE VIEW PROD.FINAL.ACCOUNT_HEALTH_SCORE AS
SELECT 
    a.ACCOUNT_NAME,
    -- Sentiment component (0-40 points)
    COALESCE(s.SENTIMENT_SCORE_NORMALIZED, 20) AS SENTIMENT_SCORE_NORMALIZED,
    -- Product coverage component (0-30 points)
    LEAST(a.PRODUCTS_OWNED * 5, 30) AS PRODUCT_COVERAGE_SCORE,
    -- Peer comparison component (0-30 points)
    -- ... additional scoring logic
FROM PROD.FINAL.ACCOUNT_DAILY a
LEFT JOIN PROD.FINAL.ACCOUNT_CALL_SENTIMENT s ON a.ACCOUNT_NAME = s.ACCOUNT_NAME;
```

This creates:
- `PROD.RAW.GONG_CALL_SENTIMENT` - Per-call sentiment scores
- `PROD.FINAL.ACCOUNT_CALL_SENTIMENT` - Account-level sentiment summary
- `PROD.FINAL.ACCOUNT_HEALTH_SCORE` - Composite health score (0-100)

---

### Step 10e: Grant Permissions

📁 **File**: `setup/10_gong_analysis/05_grant_permissions.sql`

```sql
GRANT SELECT ON TABLE PROD.RAW.GONG_TRANSCRIPT_SOURCE TO ROLE PUBLIC;
GRANT SELECT ON TABLE PROD.RAW.GONG_CALL_SENTIMENT TO ROLE PUBLIC;
GRANT SELECT ON TABLE PROD.FINAL.ACCOUNT_CALL_SENTIMENT TO ROLE PUBLIC;
GRANT SELECT ON VIEW PROD.FINAL.ACCOUNT_HEALTH_SCORE TO ROLE PUBLIC;
GRANT USAGE ON CORTEX SEARCH SERVICE PROD.FINAL.TRANSCRIPT_SEARCH TO ROLE PUBLIC;
```

---

### Step 10f: Add TranscriptSearch to Agent (UI)

After creating the Transcript Search service, add it to the agent:

1. Navigate to **AI & ML → Snowflake Intelligence**
2. Find `CUSTOMER_360_AGENT` and click the **three-dot menu** (⋮)
3. Select **Edit**
4. Scroll down to the **Tools** section
5. Click **+ Add Tool**
6. Select **Cortex Search** from the tool type dropdown
7. Configure the tool:
   - **Tool Name**: `TranscriptSearch`
   - **Cortex Search Service**: Click the dropdown and select `PROD.FINAL.TRANSCRIPT_SEARCH`
   - **Tool Description**: `Search Gong call transcripts for customer conversations, insights about fiscal planning, layoffs, tech changes, expansion opportunities, or competitive intelligence.`
   - **Max Results**: `10`
8. Click **Save** to update the agent

---

### Step 10g: Update Semantic View with Health Score (UI)

The new `ACCOUNT_HEALTH_SCORE` view provides a composite health score based on Gong sentiment analysis. Update the semantic view to use this instead of the basic health score:

1. Navigate to **AI & ML → Cortex Analyst**
2. Find `CUSTOMER_360_SEMANTIC_VIEW` and click to edit
3. **Add the Health Score table**:
   - Click **+ Add Table**
   - Select `PROD.FINAL.ACCOUNT_HEALTH_SCORE`
   - Set **Unique Key**: `ACCOUNT_NAME`
   - Add synonyms: `health`, `sentiment`
4. **Add relationship**:
   - Create relationship from `ACCOUNT_HEALTH_SCORE.ACCOUNT_NAME` to `ACCOUNT_DAILY.ACCOUNT_NAME`
5. **Remove old health dimension**:
   - Find and remove the `HEALTHSCORE` dimension from the accounts table (this was the basic health score from SFDC_ACCOUNT)
6. **Update metrics**:
   - Update `at_risk_accounts` metric to use `HEALTH_CATEGORY IN ('At Risk', 'Critical')` from health_scores table
7. Click **Save** to update the semantic view

---

## Part 3: Postgres Activity Logs Pipeline (Step 11)

Stream real-time Okta activity logs from Snowflake Postgres to Snowflake tables via Openflow CDC.

**Goal**: Track product adoption by showing license assignment rates and actual usage through authentication logs.

### Folder Structure

```
setup/11_postgres_activity_logs/
├── 01_create_network_rule.sql        # Network rule for Postgres access
├── 02_create_postgres_tables.sql     # DDL for Postgres tables
├── 03_enable_replication.sql         # Enable CDC replication
├── 04_generate_activity_data.sql     # Sample data generation
├── 05_configure_external_access.sql  # External access for Openflow
├── 06_verification_queries.sql       # Verification and analytics queries
├── 07_streaming_procedure.sql        # Real-time streaming simulator
```

---

### Step 11a: Create Network Rule (Snowflake)

📁 **File**: `setup/11_postgres_activity_logs/01_create_network_rule.sql`

Snowflake Postgres requires a network rule to allow external connections:

```sql
USE ROLE SYSADMIN;
CREATE SCHEMA IF NOT EXISTS PROD.NETWORK;

USE ROLE ACCOUNTADMIN;

-- Create network rule allowing all IPs (for demo purposes)
CREATE OR REPLACE NETWORK RULE PROD.NETWORK.POSTGRES_ACCESS_RULE
  TYPE = IPV4
  MODE = POSTGRES_INGRESS
  VALUE_LIST = ('0.0.0.0/0');

-- Create network policy referencing the rule
CREATE OR REPLACE NETWORK POLICY POSTGRES_ACCESS_POLICY
  ALLOWED_NETWORK_RULE_LIST = ('PROD.NETWORK.POSTGRES_ACCESS_RULE');

-- Verify
SHOW NETWORK POLICIES;
```

---

### Step 11b: Create Postgres Instance (Snowsight UI)

1. Navigate to **Postgres** in the left navigation menu
2. Click **+ Instance** to create a new Postgres instance
3. Configure:
   - **Name**: `okta_activity_logs`
   - **Compute Family**: BURST_S
   - **Storage**: 25 GB
   - **Postgres Version**: 18
   - **Network Policy**: `POSTGRES_ACCESS_POLICY` (created in Step 11a)
4. Click **Create** and save connection credentials securely

**Reference**: [Snowflake Postgres Documentation](https://docs.snowflake.com/en/user-guide/snowflake-postgres/about)

---

### Step 11c: Create Postgres Tables (Postgres)

📁 **File**: `setup/11_postgres_activity_logs/02_create_postgres_tables.sql`

Connect to your Postgres instance using DBeaver, psql CLI, or another Postgres client:

```bash
psql postgres://snowflake_admin:****@<your-instance>.postgres.snowflake.app:5432/postgres
```

Then create the tables:

```sql
-- Users table: Company user profiles
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    account_id VARCHAR(20) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    job_title VARCHAR(100),
    department VARCHAR(50),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Product assignments: Which users are assigned to which products
CREATE TABLE product_user_assignment (
    assignment_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(user_id),
    product_code VARCHAR(10) NOT NULL,  -- SSO, MFA
    assigned_date DATE NOT NULL DEFAULT CURRENT_DATE,
    assignment_status VARCHAR(20) DEFAULT 'active',
    UNIQUE(user_id, product_code)
);

-- Authentication logs: Semi-structured device auth events (JSONB)
CREATE TABLE device_auth_logs (
    log_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(user_id),
    event_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    auth_event JSONB NOT NULL
);
```

---

### Step 11d: Enable Replication (Postgres)

📁 **File**: `setup/11_postgres_activity_logs/03_enable_replication.sql`

Openflow CDC requires replication privileges. Run in your Postgres instance:

```sql
-- Enable replication for the admin user
ALTER USER snowflake_admin WITH REPLICATION;

-- Create publication for CDC (run AFTER creating tables)
CREATE PUBLICATION openflow_publication FOR TABLES IN SCHEMA public;

-- Verify
SELECT * FROM pg_publication;
```

---

### Step 11e: Generate Sample Data (Postgres)

📁 **File**: `setup/11_postgres_activity_logs/04_generate_activity_data.sql`

Execute in your Postgres client to populate sample users, assignments, and auth logs.

> **Note**: This script generates ~1,500 users, ~1,700 product assignments, and ~3,500 auth logs.

---

### Step 11f: Configure External Access for Openflow (Snowflake)

📁 **File**: `setup/11_postgres_activity_logs/05_configure_external_access.sql`

Back in Snowflake, create the external access integration for Openflow to connect to your Postgres instance:

```sql
USE ROLE ACCOUNTADMIN;

-- Create role and database for CDC data
CREATE ROLE IF NOT EXISTS Postgres_HOL_ROLE;
CREATE DATABASE IF NOT EXISTS Okta_PGCDC_DB;
CREATE WAREHOUSE IF NOT EXISTS Okta_PGCDC_WH
  WAREHOUSE_SIZE = MEDIUM
  AUTO_SUSPEND = 300;

-- Grant privileges
GRANT OWNERSHIP ON DATABASE Okta_PGCDC_DB TO ROLE Postgres_HOL_ROLE;
GRANT USAGE ON WAREHOUSE Okta_PGCDC_WH TO ROLE Postgres_HOL_ROLE;
GRANT ROLE Postgres_HOL_ROLE TO ROLE OPENFLOW_ADMIN;

-- Create network rule (update with your Postgres host)
CREATE OR REPLACE NETWORK RULE Okta_PGCDC_DB.NETWORKS.postgres_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('YOUR-POSTGRES-HOST:5432');  -- Replace with your Postgres endpoint

-- Create external access integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION okta_pgcdc_access
  ALLOWED_NETWORK_RULES = (Okta_PGCDC_DB.NETWORKS.postgres_network_rule)
  ENABLED = TRUE;

GRANT USAGE ON INTEGRATION okta_pgcdc_access TO ROLE Postgres_HOL_ROLE;
```

> **Important**: Replace `YOUR-POSTGRES-HOST` with your actual Postgres instance hostname.

---

### Step 11g: Configure Openflow CDC Pipeline

#### 1. Create Runtime

Navigate to **Data → Ingestion → Openflow → Runtimes**:

| Setting | Value |
|---------|-------|
| Name | `okta-activity-logs-runtime` |
| Deployment | Your Openflow deployment |
| Runtime Role | `Postgres_HOL_ROLE` |
| External Access Integration | `okta_pgcdc_access` |

#### 2. Install PostgreSQL Connector

- Go to **Openflow → Connectors** tab
- Find **PostgreSQL** in the connector list
- Click **Install** to add it to your deployment

#### 3. Add PostgreSQL Connector

Click **+ Add Connector** → **PostgreSQL**

#### 4. Configure Source Parameters

From the Parameter contexts list, edit **PostgreSQL Source Parameters**:

| Parameter | Value | Description |
|-----------|-------|-------------|
| PostgreSQL Connection URL | `jdbc:postgresql://<your-instance>.postgres.snowflake.app:5432/postgres` | JDBC connection URL |
| PostgreSQL JDBC Driver | `postgresql-42.7.7.jar` | Download from [jdbc.postgresql.org](https://jdbc.postgresql.org/download/) |
| PostgreSQL Password | `<your-password>` | Password for snowflake_admin user |
| PostgreSQL Username | `snowflake_admin` | PostgreSQL user with REPLICATION privileges |
| Publication Name | `openflow_publication` | The publication created in Step 11d |
| Replication Slot Name | *(leave empty)* | Auto-generated by Openflow |

**To upload the JDBC driver:**
1. Click the file icon next to "PostgreSQL JDBC Driver"
2. Select "Reference Asset"
3. Click "Upload" and select your downloaded `postgresql-42.7.7.jar`
4. Click Apply

#### 5. Configure Destination Parameters

Edit **PostgreSQL Destination Parameters**:

| Parameter | Value | Description |
|-----------|-------|-------------|
| Destination Database | `Okta_PGCDC_DB` | Target Snowflake database |
| Snowflake Authentication Strategy | `SNOWFLAKE_MANAGED` | Uses Snowflake managed authentication |
| Snowflake Role | `Postgres_HOL_ROLE` | Role with table creation privileges |
| Snowflake Warehouse | `Okta_PGCDC_WH` | Warehouse for data processing |
| Snowflake Account Identifier | *(leave empty)* | Not needed with session token |
| Snowflake Username | *(leave empty)* | Not needed with session token |
| Snowflake Private Key | *(leave empty)* | Not needed with session token |

#### 6. Configure Ingestion Parameters

Edit **PostgreSQL Ingestion Parameters**:

| Parameter | Value | Description |
|-----------|-------|-------------|
| Included Table Regex | `public\..*` | Matches all tables in public schema |
| Column Filter JSON | `[]` | Empty = include all columns |
| Ingestion Type | `full` | Full snapshot + incremental CDC |
| Merge Task Schedule CRON | `* * * * * ?` | Every second for near real-time |

#### 7. Enable and Start

- Right-click connector → **Enable all controller services**
- Right-click connector → **Start**
- Monitor the connector flow to watch the snapshot load

---

### Step 11h: Verify Data Flow

📁 **File**: `setup/11_postgres_activity_logs/06_verification_queries.sql`

Verify data is flowing to Snowflake:

```sql
USE ROLE Postgres_HOL_ROLE;
USE DATABASE Okta_PGCDC_DB;
USE SCHEMA "public";

-- Check tables were created
SHOW TABLES IN SCHEMA Okta_PGCDC_DB."public";

-- Verify row counts
SELECT 'USERS' as table_name, COUNT(*) as row_count FROM "users"
UNION ALL
SELECT 'PRODUCT_USER_ASSIGNMENT', COUNT(*) FROM "product_user_assignment"
UNION ALL
SELECT 'DEVICE_AUTH_LOGS', COUNT(*) FROM "device_auth_logs";
```

> **Important**: Schema and table names from Postgres are lowercase and require double quotes: `"public"`, `"users"`, `"user_id"`, etc.

---

### Step 11i: Create Streaming Procedure (Postgres)

📁 **File**: `setup/11_postgres_activity_logs/07_streaming_procedure.sql`

Connect to your Postgres instance using DBeaver, psql CLI, or another Postgres client and run the streaming procedure script.

> **Note**: This is PostgreSQL code - do NOT run in Snowflake UI.

To generate test data for real-time CDC testing:
```sql
-- Generate 100 auth logs (10 batches of 10)
CALL stream_auth_logs(10, 10);
```

---

## Sample Questions
- "Which accounts are at risk of churning?"
- "What products has Microsoft added or churned over time?"
- "Show me accounts with Critical health score and upcoming renewals"

### Pricing & Discounts
- "Which accounts have discounts above the 15% target?"
- "What is the optimal discount for a customer like Apple?"

### Lost Opportunities
- "Why did we lose the expansion deal with Google?"
- "What are the most common reasons for lost opportunities?"

### Call Transcript Insights
- "What did our last call with Microsoft reveal about their budget?"
- "Which customers mentioned layoffs in recent calls?"

### Contract Details
- "What products are included in the Walmart contract?"
- "Show me all contracts with auto-renewal disabled"

### Public Company Research (Web Search)
- "What recent news is there about Amazon's security initiatives?"
- "Find 10K filing information for JPMorgan Chase"

### Activity & Usage Analytics
- "What is the license assignment rate for SSO across accounts?"
- "Which accounts have the highest MFA adoption rates?"
- "Show authentication failures by device type"

---

## Data Summary

After completing all steps:

| Component | Count |
|-----------|-------|
| Accounts | 250 Fortune 500 companies |
| Contracts | 250 (one per account) |
| Subscriptions | ~970 (2-6 products per contract) |
| Opportunities | 500 (60% won, 40% lost) |
| Gong Transcripts | ~150 (25% of accounts) |
| Sales Teams | 11 (by territory) |
| Users (Postgres) | ~1,500 |
| Product Assignments | ~1,700 |
| Auth Logs | ~3,500+ |

---

## Identity Products

| Product | Code | Price/User/Month |
|---------|------|------------------|
| Single Sign-On (SSO) | SSO | $6.00 |
| Multi-Factor Authentication (MFA) | MFA | $3.00 |
| Adaptive MFA | AMFA | $6.00 |
| Universal Directory | UD | $2.00 |
| Lifecycle Management | LCM | $8.00 |
| API Access Management | API | $4.00 |
| Device Access | DA | $5.00 |
| Access Governance | AG | $8.00 |
| Privileged Access | PAM | $15.00 |
| Workflows | WF | $4.00 |
| Identity Threat Protection | ITP | $5.00 |
| Identity Security Posture Management | ISPM | $6.00 |
| Access Gateway | AGW | $5.00 |
| Secure Partner Access | SPA | $4.00 |

---

## Troubleshooting

### Dynamic tables not refreshing
```sql
ALTER DYNAMIC TABLE PROD.FINAL.ACCOUNT_DAILY REFRESH;
```

### Agent not appearing in Snowflake Intelligence
Ensure grants are applied and user has access to `SNOWFLAKE_INTELLIGENCE.AGENTS` schema.

### Postgres CDC not syncing
1. Verify publication exists: `SELECT * FROM pg_publication;`
2. Check replication slot: `SELECT * FROM pg_replication_slots;`
3. Verify user has REPLICATION privilege

### Case sensitivity issues with CDC tables
PostgreSQL objects sync as lowercase. Use double quotes:
```sql
-- Correct
SELECT * FROM Okta_PGCDC_DB."public"."users";

-- Incorrect (will fail)
SELECT * FROM Okta_PGCDC_DB.public.USERS;
```

---

## Step 12: Choose Your Own Adventure 🚀

This section provides ideas for extending the Customer 360 solution using **Cortex Code** - Snowflake's AI-powered CLI. Each adventure builds on what you've created and demonstrates additional Snowflake capabilities.

### Installing Cortex Code CLI

Before starting any adventure, install and configure Cortex Code:

```bash
# Install via pip
pip install snowflake-cortex-code

# Or via Homebrew (macOS)
brew install snowflake-cortex-code

# Authenticate with your Snowflake account
cortex login
```

Once installed, you can use natural language to build features:
```bash
# Start an interactive session
cortex chat

# Or run a one-off command
cortex "create a view that joins account data with auth logs"
```

### Adventure A: Data Governance & PII Detection

**Goal**: Classify and protect sensitive data in your Postgres CDC tables.

The `device_auth_logs` table contains potentially sensitive information (IP addresses, geo-location, device info). Use Snowflake's data classification to identify and protect PII.

**Prompt Cortex Code:**
```
Classify the tables in Okta_PGCDC_DB."public" for PII and sensitive data. 
Create masking policies for any identified PII fields and apply them to the 
PUBLIC role while allowing SYSADMIN to see unmasked data.
```

**What you'll learn:**
- SYSTEM$CLASSIFY for automatic PII detection
- Creating and applying masking policies
- Row access policies for geographic restrictions
- Audit logging with ACCESS_HISTORY

### Adventure B: Add Usage Analytics to Account 360

**Goal**: Integrate Postgres activity data into the semantic view for complete customer visibility.

Create views that summarize license utilization and authentication patterns, then add them to the Customer 360 semantic view.

**Prompt Cortex Code:**
```
Create a view in PROD.FINAL called ACCOUNT_USAGE_METRICS that joins the 
Okta_PGCDC_DB CDC tables with PROD.FINAL.ACCOUNT_DAILY to show:
- License assignment rate (assigned users / subscription quantity)
- Active user rate (users with auth events in last 30 days / assigned users)  
- Auth success rate
- Most common device types
Group by account and include the account name.
```

Then update your semantic view to include the new metrics.

### Adventure C: Build a Streamlit Dashboard

**Goal**: Create an interactive Customer 360 dashboard using Streamlit in Snowflake.

**Prompt Cortex Code:**
```
Create a Streamlit app that shows a Customer 360 dashboard with:
1. Account selector dropdown
2. Key metrics cards (ARR, health score, products owned)
3. Product timeline chart showing adds/churns
4. Recent Gong call sentiment summary
5. Contract renewal countdown
Use the semantic view PROD.FINAL.CUSTOMER_360_SEMANTIC_VIEW for data.
```

**What you'll learn:**
- Streamlit in Snowflake deployment
- Connecting to semantic views from Python
- Interactive visualizations with Altair/Plotly

### Adventure D: Predictive Churn Model

**Goal**: Build an ML model to predict which accounts are likely to churn.

**Prompt Cortex Code:**
```
Using Snowflake ML, create a classification model to predict account churn using:
- Health score trends
- Product coverage changes
- Gong sentiment patterns
- Authentication activity (if available)
- Days until renewal

Train on historical data where STATUS = 'Churned' as the target. 
Register the model in the Snowflake Model Registry.
```

**What you'll learn:**
- Snowflake ML model training
- Feature engineering from multiple sources
- Model Registry for versioning
- Inference in SQL with MODEL_PREDICT

### Adventure E: Automated Alerts with Tasks

**Goal**: Create proactive notifications for at-risk accounts.

**Prompt Cortex Code:**
```
Create a Snowflake Task that runs daily and:
1. Identifies accounts with health score < 50 AND renewal in next 90 days
2. Checks for negative Gong sentiment in last 30 days
3. Sends a summary to a Slack webhook or email notification
4. Logs alerts to an ACCOUNT_ALERTS table for tracking
```

**What you'll learn:**
- Snowflake Tasks and scheduling
- External functions for notifications
- Alert tracking and deduplication

### Adventure F: Executive Summary Generator

**Goal**: Use Cortex LLM functions to generate account summaries.

**Prompt Cortex Code:**
```
Create a stored procedure that generates an executive summary for any account using:
- CORTEX.COMPLETE to synthesize account data
- Recent Gong transcript highlights
- Product history and health metrics
- Upcoming renewal information

The procedure should return a formatted markdown summary suitable for 
executive review before customer meetings.
```

**What you'll learn:**
- Cortex LLM functions (COMPLETE, SUMMARIZE)
- Prompt engineering for business context
- Combining structured and unstructured data

### Adventure G: Competitive Intelligence Tracker

**Goal**: Monitor competitor mentions across customer interactions.

**Prompt Cortex Code:**
```
Create a pipeline that:
1. Searches Gong transcripts for competitor mentions (Ping Identity, Auth0, 
   Microsoft Entra, CyberArk, ForgeRock)
2. Uses CORTEX.SENTIMENT to analyze the context (positive/negative for us)
3. Stores findings in a COMPETITIVE_INTELLIGENCE table
4. Creates a view summarizing competitor threat level by account
```

**What you'll learn:**
- Text pattern matching with Cortex Search
- Sentiment analysis in context
- Building intelligence dashboards

### Adventure H: Dynamic Account Segmentation

**Goal**: Create smart customer segments that update automatically.

**Prompt Cortex Code:**
```
Create dynamic tables that segment accounts into:
- "Growth Champions" - High health, expanding product usage
- "At Risk Revenue" - High ARR but declining health/engagement  
- "Untapped Potential" - Good health, low product coverage
- "Urgent Attention" - Low health, upcoming renewal

Include recommended actions for each segment based on their characteristics.
```

**What you'll learn:**
- Dynamic tables for real-time segmentation
- Business logic in SQL
- Actionable analytics patterns

---

## Teardown / Cleanup

To completely remove all objects created in this lab:

```sql
@setup/99_teardown/teardown_all.sql
```

This script removes:
- Cortex Agent (`CUSTOMER_360_AGENT`)
- Cortex Search Services (`CONTRACT_SEARCH`, `TRANSCRIPT_SEARCH`)
- Semantic View (`CUSTOMER_360_SEMANTIC_VIEW`)
- `PROD` database (all schemas, tables, stages)
- `SNOWFLAKE_INTELLIGENCE` database
- Postgres CDC objects (`Okta_PGCDC_DB`, `Postgres_HOL_ROLE`, `Okta_PGCDC_WH`)
- Network policies and integrations

**Manual cleanup required:**
1. **Snowflake Postgres**: Delete via UI (Postgres → your instance → Delete)
2. **Openflow Runtime**: Stop and delete connectors/runtime via UI

---

## Reference Documentation

- [Snowflake Cortex Agents](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents)
- [Cortex Search](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview)
- [Semantic Views](https://docs.snowflake.com/en/user-guide/views-semantic/overview)
- [Snowflake Postgres](https://docs.snowflake.com/en/user-guide/snowflake-postgres/about)
- [Openflow CDC](https://docs.snowflake.com/en/user-guide/data-load-openflow)
