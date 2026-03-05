SUMMARY
You will create a text guide with code examples of setting up unstructured data stored in a directory for cortex search and AI SQL insights.

For all steps write them out in a consumable format with examples and links to documentation

REQUIREMENTS
1. Create a step by step guide for a user to implement themselves:
 - Storing the gong logs in /gong_transcripts/ into a stage with DIRECTORY=TRUE enabled
 - Create cortex search service on these files and give instructions to add to customer 360 agent
 - Use AI_SENTIMENT to categorize the sentiment of the call
 - Create a composite health score based on the sentiment of the calls, product whitespace, and peer comparison

OUTPUT
- Save the guide to setup/11_gong_analysis/README.md
- Also create an HTML version at setup/11_gong_analysis/README.html

CRITICAL IMPLEMENTATION NOTES (from testing):

1. DIRECTORY_TABLE LIMITATION:
   - Dynamic Tables do NOT support DIRECTORY_TABLE as a source
   - You MUST create an intermediate regular table (GONG_TRANSCRIPT_SOURCE) that reads from the stage
   - The Cortex Search Service refreshes from this source table

2. FILE FORMAT FOR READING TEXT FILES:
   - Create a TEXT_FORMAT file format with FIELD_DELIMITER=NONE and RECORD_DELIMITER=NONE
   - Query the stage using: SELECT $1::VARCHAR, METADATA$FILENAME FROM @STAGE (FILE_FORMAT => 'TEXT_FORMAT')
   - Do NOT use AI_PARSE_DOCUMENT for simple text files - it's for PDFs/documents

3. AI_SENTIMENT SYNTAX:
   - AI_SENTIMENT returns a complex object structure
   - Extract sentiment using: AI_SENTIMENT(content):categories[0]:sentiment::VARCHAR
   - Returns 'positive', 'negative', or 'neutral' as strings
   - Convert to numeric: CASE WHEN sentiment = 'positive' THEN 1 WHEN sentiment = 'negative' THEN -1 ELSE 0 END

4. TRANSCRIPT CONTENT MUST VARY:
   - If all transcripts have similar content, AI_SENTIMENT returns the same result for all
   - The generate_transcripts.py script must create varied content based on account health
   - Use different conversation templates for positive/negative/neutral scenarios
   - Map account health scores to sentiment probability (e.g., healthy accounts = mostly positive calls)

5. HEALTH SCORE FORMULA (tested and balanced):
   - Weights: Sentiment 50%, Product Coverage 20%, Peer Comparison 30%
   - Product Coverage needs boost: LEAST(PRODUCT_COVERAGE * 300, 100) because most accounts have few products
   - Peer Comparison boost: accounts at/above industry average get 70-100, below get 40-70
   - Thresholds: Excellent >= 70, Good >= 60, At Risk >= 50, Critical < 50

6. CORTEX SEARCH SERVICE REQUIREMENTS:
   - Requires a regular table as source (not directory table)
   - Must specify TARGET_LAG for refresh interval
   - Embedding model: 'snowflake-arctic-embed-l-v2.0'

7. SEMANTIC VIEW INTEGRATION:
   - Add ACCOUNT_HEALTH_SCORE to the semantic model YAML
   - Include dimensions: ACCOUNT_NAME, INDUSTRY, HEALTH_CATEGORY
   - Include measures: all score components plus call counts
   - Recreate semantic view after YAML update

SCHEMA LOCATIONS:
- Stage and source tables: PROD.RAW
- Search service: PROD.RAW
- Sentiment tables: PROD.RAW (raw), PROD.FINAL (aggregated)
- Health score view: PROD.FINAL
- Semantic view: PROD.FINAL.CUSTOMER_360_SEMANTIC_VIEW
