SUMMARY
You will create and end to end Demo for customer 360 for an identify company like Okta.  The goal of the lab is to answer questions about customers and sales patches to get cross sell and upsell opportunities, pdf contract review, call transcripts and support cases for overall account health and propencity to buy / expand.  Each of these data points will be stored in their native formats (structrued or unstructured) and will be consumed from within Snowflake Intillegce through Agents.

For all steps write them into a file structure including DDL, data generated etc. so that I could reproduce this on my own in a different account.  This is designed to be a hands on lab that I am leading for 40 other users to implement step by step. 

REQUIREMENTS
1. Create a prod database and raw schema using the /Source_Table_DDLs/source_ddl.rtf as reference.  This is the basis for cusotmer informaiton coming salesforce.  You do not need every field.  Fill out enough to have a full picture of a customers account, product sku's they use, any relevant subscriptions.  This should be a few hundred accounts.  Keep them USA based and spread them across the major timezones to create regional distiction.  Make sure to populate failed opportunities for expansion where a product was proposed with a contract and declined.  This will be used for account health and expansion.
2. Create a sales account team table that maps an account executive, solutions engineer and Sales development representative to each account. Keep the account alignment to roughly 20 per account team and make sure to map them to regional territories based on timezones in step 1. 
3. Create SaaS pdf contracts and store them in a directory internal stage which maps to the opportunities created in step 1.  The PDFs should have the customer informaiton (comapny name, buyer, address) and then a table made of idntity service products (API, MFS, SSO) and the associated price (base and discount). The overall contract value should map to the value stored in the table.
4. Create a schema called final with tables using /Source_Table_DDLs/transformed_source_ddls.rtf.  These tables are the downstream result from the tables in step 1 raw schema.  The field names should match, again doesn't have to be all fields but should have a full account, contract, product sku and sales team infromation.  Create dynamic tables to transform the data between the raw schema and the final schema tables. 
5. Generate gong call transcripts that map to the customer names you created in step 1.  Only do this for 25% of the total customer population.  Make sure to name the call transcript files as the customer name with a date appended. Have some of the trasncripts provide insights about the customers fiscal planning, layoffs, tech changes etc. Do nothing other than create the files and store them in the data generation folder in the repo.
6. Create cortex agents for snowflake intelligence and use the sample questions section to help structure the semantic views.  The unstructured data should stay unstrcutred and be vectorized for cortex search. There should also be a way to search for publicly avaialble informaiton about the compnay (linkedin posts, 10K or 10Q, major business deals etc.)

Sample Questions For Agents:
-What products has my customer added or churned over time?
-How has my customer's pricing and discounts changed over time?  Are they moving closer to the recommended discount?
-I need to know if my customer was given a discount for buying product X, so if they churn the product I know to remove it.
-What is the optimal discount for a customer like this given the products they have?
-What would be the recommended next best product for this customer based on what other similar customers have?
-Do any related customers (ie other accounts rolling up to the same parent account) have contracts with us and what products/pricing do they have?

References:
- For PDF contract creation and product sku names: https://www.okta.com/pricing/
- Customer data should be from Fortune 500 customers: https://www.50pros.com/fortune500 