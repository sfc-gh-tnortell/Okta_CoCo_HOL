
SUMMARY
You will create a text guide with code examples of setting up a pipeline to mimic Okta activity logs that map to the products in the PROD.RAW.SFDC_PRODUCT table.  The pipeline will include postgres hosted in Snowflake, an openflow process to stream them into Snowflake tables.

For all steps write them out in a consumable format with examples and links to documentation.

The goal is to showcase usage logs of products that have been purchased and their overall adoption rate at the user level.  I should be able to see if a company has assigned all their licenses to users and the rate at which the assigned users have enrolled and are using it.  If there is an authentiction log I can determine that user is using the product they were assigned. 


REQUIREMENTS
1. Use the referenced link as the basis for the structure.
2. The postgres tables should be the following:
- Users: Company user profiles mapped to the companies created in prompt.md setup. Include things like name, email, company role etc. things that could be useful to distinguish them at a given company. the number of users per company should match the subscription amount purchased that was created earlier.  So if a company purchased 100 SSO licenses I'd want between 70 and 100 users created.
- Device authentication logs: use the product table and generate authentication logs for the company users. Include things like auth status (failed, denied, passed etc.), the device type (mobile, browser etc.) and the authentication type.  Focus just on MFA and SSO as products. These should be in semi-structured format like JSON.  Do not have all users in each company have a log.  I need to show feature adoption by usage
- Product User Assignment:  This should be a map between Users and product(MFA & SSO) showing their assignment to a product, the assigned date and the expiration date of the license.
3. There is an existing Openflow deployment called Openflow_Deployment.  Specify creating a new runtime with the postgres connector. 
4. Create a secondary dataset from step 2 and provide a process to mimic streaming data into the postgres tables.  
5. You do not need to create anything after step 4 eventhough the referenced material creates things like cortex agents. 




Questions:
- does you have it configured and are you using it 
- do you have MFA enabled, how many apps 
- how many users have those apps
- I have MFA enabled, here's how many users I have provisioned total, who has opted in 

References:
Postgres Snowflake and Openflow: https://www.snowflake.com/en/developers/guides/getting-started-with-snowflake-postgres/