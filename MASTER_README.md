# Okta Customer 360 Demo - Complete Setup Guide

This comprehensive guide covers the full setup of the Okta Customer 360 demo environment for Snowflake Intelligence, including core data setup, Gong transcript analysis, web search integration, and real-time activity log streaming via Snowflake Postgres and Openflow CDC.

---

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Part 1: Core Setup (Steps 1-9)](#part-1-core-setup-steps-1-9)
4. [Part 2: Gong Transcript Analysis (Step 10)](#part-2-gong-transcript-analysis-step-10)
5. [Part 3: Web Search Integration (Step 11)](#part-3-web-search-integration-step-11)
6. [Part 4: Postgres Activity Logs Pipeline (Step 12)](#part-4-postgres-activity-logs-pipeline-step-12)
7. [Sample Questions](#sample-questions)
8. [Data Summary](#data-summary)
9. [Troubleshooting](#troubleshooting)

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
│  │  │(Semantic View)│  │(Transcripts) │  │(Public Company Info)  │ │   │
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
- For Step 12: Existing Openflow deployment

---

## Part 1: Core Setup (Steps 1-9)

### Folder Structure

```
setup/
├── 01_database/          # Create database and schemas
├── 02_raw_tables/        # Create source tables (SFDC_* + GONG)
├── 03_data_generation/   # Generate synthetic data
├── 04_sales_teams/       # Create sales team mappings
├── 05_pdf_contracts/     # Parse PDF contracts and create search service
├── 06_final_schema/      # Create dynamic tables
├── 07_semantic_view/     # Create semantic view for Cortex Analyst
├── 08_agent/             # Create Cortex Agent
├── 09_grants/            # Grant permissions
```

### Step 1: Database Setup

```sql
@setup/01_database/01_create_database.sql
```

### Step 2: Create Raw Tables

```sql
@setup/02_raw_tables/create_raw_tables.sql
```

### Step 3: Generate Data

```sql
@setup/03_data_generation/01_insert_products.sql
@setup/03_data_generation/02_insert_accounts.sql
@setup/03_data_generation/03_fix_timezone_distribution.sql
@setup/03_data_generation/04_insert_contracts.sql
@setup/03_data_generation/05_insert_subscriptions.sql
@setup/03_data_generation/06_insert_opportunities.sql
@setup/03_data_generation/07_insert_gong_transcripts.sql
```

### Step 4: Sales Teams

```sql
@setup/04_sales_teams/01_create_sales_team_table.sql
@setup/04_sales_teams/02_insert_sales_teams.sql
```

### Step 5: PDF Contracts Pipeline

This step uploads PDF contracts, parses them, and creates a Cortex Search service.

**5a: Create Stage**
```sql
@setup/05_pdf_contracts/01_create_stage.sql
```

Then upload the PDFs via Snowsight:
1. Navigate to **Data → Databases → PROD → RAW → Stages**
2. Click on `CONTRACTS_STAGE`
3. Click **+ Files** → Select all PDF files from `unstructured_data/contracts_pdf/`
4. Click **Upload**

**5b: Parse PDFs into Source Table**
```sql
@setup/05_pdf_contracts/02_create_source_table.sql
```

**5c: Create Enriched Content Table**
```sql
@setup/05_pdf_contracts/03_create_content_table.sql
```

**5d: Create Cortex Search Service**
```sql
@setup/05_pdf_contracts/04_create_search_service.sql
```

### Step 6: Final Schema (Dynamic Tables)

```sql
@setup/06_final_schema/01_create_final_schema.sql
@setup/06_final_schema/02_create_account_daily_dt.sql
@setup/06_final_schema/03_create_subscription_daily_dt.sql
@setup/06_final_schema/04_create_opportunity_daily_dt.sql
```

### Step 7: Semantic View & Cortex Analyst

Create the semantic view and configure Cortex Analyst to enable natural language queries.

**7a: Create Semantic View**
```sql
@setup/07_semantic_view/01_create_semantic_view.sql
```

**7b: Create Cortex Analyst in Snowsight**
1. Navigate to **AI & ML → Cortex Analyst**
2. Click **+ Analyst**
3. Configure:
   - **Name**: `Customer 360 Analyst`
   - **Semantic View**: Select `PROD.FINAL.CUSTOMER_360_SEMANTIC_VIEW`
4. Click **Create**
5. Test with a sample question like "Show me accounts at risk of churning"

### Step 8: Cortex Agent

Create the Cortex Agent for Snowflake Intelligence with access to all data sources.

**Option A: Via SQL**
```sql
@setup/08_agent/01_create_agent_schema.sql
@setup/08_agent/02_create_agent.sql
```

**Option B: Via Snowsight UI**
1. Navigate to **AI & ML → Snowflake Intelligence → Agents**
2. Click **+ Agent**
3. Configure basic settings:
   - **Name**: `CUSTOMER_360_AGENT`
   - **Database**: `SNOWFLAKE_INTELLIGENCE`
   - **Schema**: `AGENTS`
   - **Model**: `claude-3-5-sonnet`
4. Add tools:

   **Tool 1 - Cortex Analyst (Semantic View)**:
   - Click **+ Add Tool** → **Cortex Analyst**
   - Select semantic view: `PROD.FINAL.CUSTOMER_360_SEMANTIC_VIEW`
   
   **Tool 2 - Contract Search**:
   - Click **+ Add Tool** → **Cortex Search**
   - Name: `contract_search`
   - Service: `PROD.FINAL.CONTRACT_SEARCH`
   - Description: `Search contract documents for specific terms, customer names, products, or pricing information.`
   - Max Results: `10`
   
   **Tool 3 - Transcript Search**:
   - Click **+ Add Tool** → **Cortex Search**
   - Name: `transcript_search`
   - Service: `PROD.FINAL.TRANSCRIPT_SEARCH`
   - Description: `Search Gong call transcripts for customer conversations and business insights.`
   - Max Results: `10`
   
   **Tool 4 - Web Search**:
   - Click **+ Add Tool** → **Web Search**
   - Name: `company_research`
   - Description: `Search for publicly available information about companies including news and filings.`

5. Add system prompt (see `02_create_agent.sql` for full prompt)
6. Click **Create**

**Verify in Snowflake Intelligence:**
1. Navigate to **AI & ML → Snowflake Intelligence**
2. The `CUSTOMER_360_AGENT` should appear in the agent list
3. Click to open and test with a sample question like "Which accounts are at risk?"

### Step 9: Permissions

```sql
@setup/09_grants/01_grant_permissions.sql
```

---

## Part 2: Gong Transcript Analysis (Step 10)

This step creates a semantic search service over Gong call transcripts and builds a composite account health score.

### Folder Structure

```
setup/10_gong_analysis/
├── 01_create_stage.sql           # Create stage and upload transcripts
├── 02_create_source_table.sql    # Parse transcripts into table
├── 03_create_search_service.sql  # Cortex Search on transcripts
├── 04_create_sentiment_table.sql # Extract sentiment metadata
├── 05_create_sentiment_summary.sql # Account-level sentiment
├── 06_create_health_score.sql    # Composite health score view
└── README.md                     # Detailed guide
```

### Step 10a: Create Transcript Stage

```sql
@setup/10_gong_analysis/01_create_stage.sql
```

Then upload `.txt` files via Snowsight:
1. Navigate to **Data → Databases → PROD → RAW → Stages**
2. Click on `GONG_TRANSCRIPTS_STAGE`
3. Click **+ Files** → Select all `.txt` files from `unstructured_data/gong_transcripts/`
4. Click **Upload**

### Step 10b: Create Source Table

```sql
@setup/10_gong_analysis/02_create_source_table.sql
```

### Step 10c: Create Search Service

```sql
@setup/10_gong_analysis/03_create_search_service.sql
```

### Step 10d: Create Sentiment Table

```sql
@setup/10_gong_analysis/04_create_sentiment_table.sql
```

### Step 10e: Create Sentiment Summary

```sql
@setup/10_gong_analysis/05_create_sentiment_summary.sql
```

### Step 10f: Create Health Score View

```sql
@setup/10_gong_analysis/06_create_health_score.sql
```

### Step 10g: Add to Semantic View

Add the `ACCOUNT_HEALTH_SCORE` table to your semantic view YAML (see `README.md` in `10_gong_analysis/` for full YAML snippet).

---

## Part 3: Web Search Integration (Step 11)

Enable web search for your Cortex Agent to access public company information, news, and 10K filings.

### Folder Structure

```
setup/11_web_search/
└── README.md                     # Step-by-step UI guide
```

### Step 11a: Enable Web Search at Account Level

1. Sign in to **Snowsight**
2. Navigate to **AI & ML → Agents → Settings**
3. Toggle **Web search** to enable

### Step 11b: Add Web Search Tool to Agent

Via SQL:

```sql
CREATE OR REPLACE CORTEX AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.CUSTOMER_360_AGENT
COMMENT = 'Customer 360 Agent with web search enabled'
MODEL = 'claude-3-5-sonnet'
TOOLS = (
    {
        'type': 'cortex_analyst_text_to_sql',
        'semantic_view': 'PROD.FINAL.CUSTOMER_360_SEMANTIC_VIEW'
    },
    {
        'type': 'cortex_search',
        'name': 'transcript_search',
        'cortex_search_service': 'PROD.FINAL.TRANSCRIPT_SEARCH',
        'description': 'Search call transcripts for customer insights',
        'max_results': 10
    },
    {
        'type': 'web_search',
        'name': 'company_research',
        'description': 'Search for publicly available information about companies'
    }
)
SYSTEM_PROMPT = 'You are a Customer 360 AI assistant with access to structured data, call transcripts, and web search.';
```

---

## Part 4: Postgres Activity Logs Pipeline (Step 12)

Stream real-time Okta activity logs from Snowflake Postgres to Snowflake tables via Openflow CDC.

### Folder Structure

```
setup/12_postgres_activity_logs/
├── 01_create_network_rule.sql     # Network rule for Postgres
├── 02_create_postgres_tables.sql  # DDL for Postgres tables
├── 03_enable_replication.sql      # Enable CDC replication
├── 04_configure_external_access.sql # External access for Openflow
├── 05_verification_queries.sql    # Verification and analytics queries
├── generate_activity_data.sql     # Sample data generation
└── README.md                      # Detailed guide
```

### Step 12a: Create Network Rule

```sql
@setup/12_postgres_activity_logs/01_create_network_rule.sql
```

### Step 12b: Create Postgres Instance (Snowsight UI)

1. Navigate to **Data → Databases → + Database**
2. Select **Postgres** as the database type
3. Configure:
   - **Name**: `okta_activity_logs`
   - **Compute Family**: BURST_S
   - **Storage**: 25 GB
   - **Network Policy**: `POSTGRES_ACCESS_POLICY`
4. Save connection credentials

### Step 12c: Create Postgres Tables

Connect to your Postgres instance and run:

```sql
@setup/12_postgres_activity_logs/02_create_postgres_tables.sql
```

### Step 12d: Enable Replication

```sql
@setup/12_postgres_activity_logs/03_enable_replication.sql
```

### Step 12e: Generate Sample Data

```sql
@setup/12_postgres_activity_logs/generate_activity_data.sql
```

### Step 12f: Configure External Access for Openflow

Update the network rule with your Postgres host, then run:

```sql
@setup/12_postgres_activity_logs/04_configure_external_access.sql
```

### Step 12g: Configure Openflow CDC Pipeline

1. **Create Runtime**: Navigate to **Data → Ingestion → Openflow → Runtimes**
   - Name: `okta-activity-logs-runtime`
   - Deployment: Your Openflow deployment
   - Runtime Role: `Postgres_HOL_ROLE`
   - External Access Integration: `okta_pgcdc_access`

2. **Add PostgreSQL Connector**: Click **+ Add Connector** → **PostgreSQL**

3. **Configure Source Parameters**:
   | Parameter | Value |
   |-----------|-------|
   | PostgreSQL Connection URL | `jdbc:postgresql://<host>:5432/postgres` |
   | PostgreSQL JDBC Driver | `postgresql-42.7.7.jar` |
   | PostgreSQL Username | `snowflake_admin` |
   | Publication Name | `openflow_publication` |

4. **Configure Destination Parameters**:
   | Parameter | Value |
   |-----------|-------|
   | Destination Database | `Okta_PGCDC_DB` |
   | Snowflake Authentication Strategy | `SNOWFLAKE_MANAGED` |
   | Snowflake Role | `Postgres_HOL_ROLE` |
   | Snowflake Warehouse | `Okta_PGCDC_WH` |

5. **Configure Ingestion Parameters**:
   | Parameter | Value |
   |-----------|-------|
   | Included Table Regex | `public\..*` |
   | Ingestion Type | `full` |

6. **Enable and Start**:
   - Right-click connector → **Enable all controller services**
   - Right-click connector → **Start**

### Step 12h: Verify Data Flow

```sql
@setup/12_postgres_activity_logs/05_verification_queries.sql
```

> **Important**: Schema and table names from Postgres are lowercase and require double quotes: `"public"`, `"users"`, `"user_id"`, etc.

---

## Sample Questions

### Account Health & Expansion
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

## Reference Documentation

- [Snowflake Cortex Agents](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents)
- [Cortex Search](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview)
- [Snowflake Postgres](https://docs.snowflake.com/en/user-guide/snowflake-postgres/about)
- [Openflow CDC](https://docs.snowflake.com/en/user-guide/data-load-openflow)
