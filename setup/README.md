# Okta Customer 360 Demo - Setup Guide

This guide will help you recreate the Customer 360 demo environment for Snowflake Intelligence. The demo uses Fortune 500 companies as customers for an identity management SaaS company (similar to Okta).

## What You'll Build

- **250 Fortune 500 customer accounts** with realistic CRM data
- **250 contracts** with product subscriptions and pricing
- **~970 subscriptions** across 14 identity products (SSO, MFA, PAM, etc.)
- **500 opportunities** including failed expansion attempts with loss reasons
- **~150 Gong call transcripts** with business insights (fiscal planning, layoffs, tech changes)
- **Cortex Search Services** for semantic search over contracts and transcripts
- **Semantic View** for natural language SQL queries
- **Cortex Agent** for Snowflake Intelligence

## Prerequisites

- Snowflake account with Cortex features enabled
- SYSADMIN or equivalent role
- COMPUTE_WH warehouse (or modify scripts to use your warehouse)
- Python 3.x with packages (for PDF/transcript generation):
  ```bash
  pip install snowflake-connector-python reportlab
  ```

## Folder Structure

```
setup/
├── 01_database/          # Create database and schemas
├── 02_raw_tables/        # Create source tables (SFDC_* + GONG)
├── 03_data_generation/   # Generate synthetic data
├── 04_sales_teams/       # Create sales team mappings
├── 05_pdf_contracts/     # Generate PDFs and transcripts (Python)
├── 06_final_schema/      # Create dynamic tables
├── 07_cortex_search/     # Set up Cortex Search services
├── 08_semantic_view/     # Create semantic view for Cortex Analyst
├── 09_agent/             # Create Cortex Agent
├── 10_grants/            # Grant permissions
```

## Execution Order

### Step 1: Database Setup
```sql
-- Run in Snowflake
@01_database/01_create_database.sql
```

### Step 2: Create Raw Tables
```sql
@02_raw_tables/01_create_account_table.sql
@02_raw_tables/02_create_product_table.sql
@02_raw_tables/03_create_contract_table.sql
@02_raw_tables/04_create_subscription_table.sql
@02_raw_tables/05_create_opportunity_table.sql    -- NEW: For expansion tracking
@02_raw_tables/06_create_gong_transcript_table.sql -- NEW: For call transcripts
```

### Step 3: Generate Data
```sql
@03_data_generation/01_insert_products.sql
@03_data_generation/02_insert_accounts.sql        -- Fortune 500 companies
@03_data_generation/03_fix_timezone_distribution.sql
@03_data_generation/04_insert_contracts.sql
@03_data_generation/05_insert_subscriptions.sql
@03_data_generation/06_insert_opportunities.sql   -- Won + Lost opportunities
@03_data_generation/07_insert_gong_transcripts.sql -- Call transcripts with insights
```

### Step 4: Sales Teams
```sql
@04_sales_teams/01_create_sales_team_table.sql
@04_sales_teams/02_insert_sales_teams.sql
```

### Step 5: Generate PDFs and Transcripts (Python)
```bash
cd setup/05_pdf_contracts

# Set your Snowflake connection
export SNOWFLAKE_CONNECTION_NAME=your_connection_name

# Generate PDF contracts
python generate_contracts.py

# Generate Gong transcript files (stored locally)
python generate_transcripts.py
```

### Step 6: Final Schema (Dynamic Tables)
```sql
@06_final_schema/01_create_final_schema.sql
@06_final_schema/02_create_account_daily_dt.sql
@06_final_schema/03_create_subscription_daily_dt.sql
@06_final_schema/04_create_opportunity_daily_dt.sql
```

### Step 7: Cortex Search Services
```sql
@07_cortex_search/01_create_contract_content.sql
@07_cortex_search/02_create_transcript_content.sql
@07_cortex_search/03_create_contract_search.sql
@07_cortex_search/04_create_transcript_search.sql
```

### Step 8: Semantic View
```sql
@08_semantic_view/01_create_semantic_view.sql
```

### Step 9: Cortex Agent
```sql
@09_agent/01_create_agent_schema.sql
@09_agent/02_create_agent.sql
```

### Step 10: Permissions
```sql
@10_grants/01_grant_permissions.sql
```

## Identity Products (based on Okta pricing)

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

## Sample Questions for the Agent

### Account Health & Expansion
- "Which accounts are at risk of churning?"
- "What products has Microsoft added or churned over time?"
- "What are the expansion opportunities for accounts in the Technology industry?"
- "Show me accounts with Critical health score and upcoming renewals"

### Pricing & Discounts
- "Which accounts have discounts above the 15% target?"
- "How has Amazon's pricing and discounts changed over time?"
- "What is the optimal discount for a customer like Apple given their product mix?"

### Lost Opportunities
- "Why did we lose the expansion deal with Google?"
- "What are the most common reasons for lost opportunities?"
- "Which competitors are winning the most deals?"

### Call Transcript Insights
- "What did our last call with Microsoft reveal about their budget situation?"
- "Which customers mentioned layoffs in recent calls?"
- "Find calls where customers discussed competitive evaluations"

### Contract Details
- "What products are included in the Walmart contract?"
- "Show me all contracts with auto-renewal disabled"
- "What is the total contract value for accounts in the Financial Services industry?"

### Public Company Research
- "What recent news is there about Amazon's security initiatives?"
- "Find 10K filing information for JPMorgan Chase"

## Data Summary

After running all scripts:
- **250 accounts**: Fortune 500 companies across 4 US timezones
- **250 contracts**: One per account with product subscriptions
- **~970 subscriptions**: 2-6 products per contract
- **500 opportunities**: 60% won, 40% lost with loss reasons
- **~150 transcripts**: 25% of accounts have Gong call records
- **11 sales teams**: Mapped by territory (West, Mountain, Central, East)

## Customization

- **Warehouse**: Modify `COMPUTE_WH` in scripts for different warehouse
- **Account Count**: Adjust in `02_insert_accounts.sql`
- **Product Pricing**: Update in `01_insert_products.sql`
- **Transcript Percentage**: Change `MOD(ABS(HASH(ACCOUNT_ID)), 4) = 0` to adjust coverage

## Troubleshooting

### "Input files from stages with Client Side Encryption is not supported"
The demo uses text-based contract content instead of parsing PDFs. The CONTRACT_CONTENT table stores searchable text representation.

### Dynamic tables not refreshing
Check that COMPUTE_WH is running and has sufficient credits. Use:
```sql
ALTER DYNAMIC TABLE PROD.FINAL.ACCOUNT_DAILY REFRESH;
```

### Agent not appearing in Snowflake Intelligence
Ensure grants are applied and user has access to SNOWFLAKE_INTELLIGENCE.AGENTS schema.
