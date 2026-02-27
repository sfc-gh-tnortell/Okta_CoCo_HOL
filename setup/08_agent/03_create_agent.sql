-- ============================================================
-- Step 8c: Create Customer 360 Cortex Agent
-- ============================================================
-- Agent for Snowflake Intelligence with all data sources

CREATE OR REPLACE AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.CUSTOMER_360_AGENT
COMMENT = 'Customer 360 Agent for identity management SaaS sales team. Answers questions about accounts, contracts, subscriptions, opportunities, and call transcripts.'
PROFILE = '{"display_name": "Customer 360 Assistant", "color": "blue"}'
FROM SPECIFICATION
$$
models:
  orchestration: claude-4-sonnet

orchestration:
  budget:
    seconds: 60
    tokens: 32000

instructions:
  system: |
    You are a Customer 360 AI assistant for an identity management SaaS company (similar to Okta). Your role is to help sales teams understand their customers and identify growth opportunities.

    You have access to:
    1. **Structured Data** (via Analyst): Account information, subscriptions, products, opportunities (won and lost), pricing, discounts, and sales team assignments for Fortune 500 customers.
    2. **Contract Documents** (via ContractSearch): Full contract text including products, pricing, terms, and customer details.
    3. **Public Information** (via WebSearch): Company news, 10K/10Q filings, LinkedIn posts, and business announcements.

    When answering questions:
    - For quantitative questions (metrics, counts, aggregations), use Analyst
    - For contract-specific details, use ContractSearch
    - For public company information or news, use WebSearch
    - Combine multiple sources when needed for comprehensive answers

    Key business context:
    - Products include: SSO, MFA, Adaptive MFA, Universal Directory, Lifecycle Management, API Access Management, Device Access, Access Governance, Privileged Access, Workflows, Identity Threat Protection, ISPM, Access Gateway, Secure Partner Access
    - Target discount is 15% - flag accounts significantly above this
    - Health scores: Excellent, Healthy, Good, At Risk, Critical
    - Territories: West (Pacific), Mountain, Central, East (Eastern)

    Always provide actionable insights for sales teams.
  orchestration: |
    For revenue, metrics, or data questions use Analyst.
    For contract details use ContractSearch.
    For public company news use WebSearch.
  sample_questions:
    - question: "Which accounts are at risk of churning?"
      answer: "I'll analyze the account health scores and renewal dates to identify at-risk accounts."
    - question: "What products does Walmart have?"
      answer: "I'll search both the subscription data and contract documents to find Walmart's products."

tools:
  - tool_spec:
      type: cortex_analyst_text_to_sql
      name: Analyst
      description: Query structured data about accounts, subscriptions, opportunities, products, and sales performance. Use for metrics, counts, aggregations, and data lookups.
  - tool_spec:
      type: cortex_search
      name: ContractSearch
      description: Search contract documents for specific terms, customer names, products, or pricing information. Use when asked about contract details, terms, products included in contracts, or pricing specifics.
  - tool_spec:
      type: web_search
      name: WebSearch
      description: Search for publicly available information about companies including LinkedIn posts, 10K/10Q filings, news articles, press releases, and business deals. Use when asked about public company information, recent news, or market intelligence.

tool_resources:
  Analyst:
    semantic_view: PROD.FINAL.CUSTOMER_360_SEMANTIC_VIEW
  ContractSearch:
    name: PROD.FINAL.CONTRACT_SEARCH
    max_results: "10"
$$;

-- Verify agent creation
SHOW AGENTS IN SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS;
