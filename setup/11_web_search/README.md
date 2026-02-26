# Enable Public Web Search in Snowflake Intelligence

This guide walks through enabling web search capabilities for Cortex Agents in Snowflake Intelligence.

---

## Prerequisites

- ACCOUNTADMIN role access (required for account-level settings)
- An existing Cortex Agent configured

---

## Step 1: Enable Web Search at the Account Level

Web search must first be enabled at the account level by an ACCOUNTADMIN before any agents can use it.

1. Sign in to **Snowsight**
2. In the navigation menu, select **AI & ML » Agents**
3. Select **Settings** (gear icon)
4. Toggle **Web search** to enable the feature

> **Note**: This is a one-time account-level setting. Once enabled, individual agents can be configured to use web search.

---

## Step 2: Add Web Search Tool to Your Agent

### Option A: Via Snowsight UI

1. Navigate to **AI & ML » Agents**
2. Select your agent from the list
3. Select **Edit**
4. Select **Tools**
5. Find **Web search** and toggle it to enable
6. Select **Save**

### Option B: Via SQL

```sql
CREATE OR REPLACE CORTEX AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.CUSTOMER_360_AGENT
COMMENT = 'Customer 360 Agent with web search enabled'
MODEL = 'claude-3-5-sonnet'
TOOLS = (
    -- Semantic View for structured data
    {
        'type': 'cortex_analyst_text_to_sql',
        'semantic_view': 'PROD.FINAL.CUSTOMER_360_SEMANTIC_VIEW'
    },
    -- Cortex Search for unstructured data
    {
        'type': 'cortex_search',
        'name': 'transcript_search',
        'cortex_search_service': 'PROD.FINAL.TRANSCRIPT_SEARCH',
        'description': 'Search call transcripts for customer insights',
        'max_results': 10
    },
    -- Web Search for public information
    {
        'type': 'web_search',
        'name': 'company_research',
        'description': 'Search for publicly available information about companies including LinkedIn posts, 10K/10Q filings, news articles, press releases, and business deals. Use this when asked about public company information, recent news, or market intelligence.'
    }
)
SYSTEM_PROMPT = 'You are a Customer 360 AI assistant. You have access to web search for finding public company information, news, and market intelligence.';
```

---

## Step 3: Update Agent System Prompt (Recommended)

Update your agent's system prompt to guide when to use web search:

```sql
ALTER CORTEX AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.CUSTOMER_360_AGENT
SET SYSTEM_PROMPT = 'You are a Customer 360 AI assistant for an identity management SaaS company.

You have access to:
1. **Structured Data** (via Cortex Analyst): Account information, subscriptions, products, opportunities
2. **Call Transcripts** (via Cortex Search): Gong call recordings with customer insights
3. **Public Information** (via Web Search): Company news, 10K/10Q filings, LinkedIn posts, press releases

When answering questions:
- For quantitative questions (metrics, counts, aggregations), use the semantic view
- For customer sentiment or business context, search transcripts
- For public company information, recent news, or market intelligence, use web search
- Combine multiple sources when needed for comprehensive answers';
```

---

## Step 4: Test Web Search Functionality

Test the web search tool with sample questions:

1. Navigate to your agent in **AI & ML » Agents**
2. Open the agent playground
3. Try questions like:
   - "What recent news is there about [Company Name]?"
   - "Find the latest 10K filing for [Company Name]"
   - "What are recent LinkedIn posts about [Company Name]?"
   - "Search for recent press releases from [Company Name]"

---

## Step 5: Verify Access Grants

Ensure users have appropriate access:

```sql
-- Grant usage on the agent to user roles
GRANT USAGE ON AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.CUSTOMER_360_AGENT 
TO ROLE SALES_ANALYST;

-- Verify grants
SHOW GRANTS ON AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.CUSTOMER_360_AGENT;
```

---

## Privacy and Legal Notes

- Snowflake processes web search inputs according to the [Snowflake Privacy Notice](https://www.snowflake.com/en/legal/privacy/privacy-policy/#2)
- Web search may not be used for redistributing or creating a competing web search service
- Web search results are from public sources and should be verified for accuracy

---

## Summary Checklist

| Step | Action | Role Required |
|------|--------|---------------|
| 1 | Enable web search at account level | ACCOUNTADMIN |
| 2 | Add web search tool to agent | Agent owner |
| 3 | Update system prompt | Agent owner |
| 4 | Test functionality | Agent user |
| 5 | Grant access to users | Agent owner |

---

## Reference Documentation

- [Cortex Agents Overview](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents)
- [Configure and Interact with Agents](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-manage)
- [Snowflake Intelligence Overview](https://docs.snowflake.com/en/user-guide/snowflake-cortex/snowflake-intelligence)
