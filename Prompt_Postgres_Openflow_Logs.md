
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


References:
Postgres Snowflake and Openflow: https://www.snowflake.com/en/developers/guides/getting-started-with-snowflake-postgres/


---

IMPLEMENTATION NOTES (Lessons Learned)

The following corrections and clarifications were discovered during implementation:

## PostgreSQL Syntax
- Publication syntax for CDC: Use `CREATE PUBLICATION openflow_publication FOR TABLES IN SCHEMA public;` (NOT `FOR ALL TABLES IN SCHEMA`)

## Step 4: External Access Configuration
Create a dedicated database, role, and warehouse for the Openflow runtime instead of using existing PROD objects:

```sql
USE ROLE ACCOUNTADMIN;

-- Create runtime role
CREATE ROLE IF NOT EXISTS Postgres_HOL_ROLE;

-- Create database for okta log data
CREATE DATABASE IF NOT EXISTS Okta_PGCDC_DB;

-- Create warehouse for data processing
CREATE WAREHOUSE IF NOT EXISTS Okta_PGCDC_WH
  WAREHOUSE_SIZE = MEDIUM
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE;

-- Grant privileges to runtime role
GRANT OWNERSHIP ON DATABASE Okta_PGCDC_DB TO ROLE Postgres_HOL_ROLE;
GRANT OWNERSHIP ON SCHEMA Okta_PGCDC_DB.PUBLIC TO ROLE Postgres_HOL_ROLE;
GRANT USAGE ON WAREHOUSE Okta_PGCDC_WH TO ROLE Postgres_HOL_ROLE;

-- Grant runtime role to OpenFlow admin
GRANT ROLE Postgres_HOL_ROLE TO ROLE OPENFLOW_ADMIN;

-- Create schema for network rules
USE ROLE Postgres_HOL_ROLE;
CREATE SCHEMA IF NOT EXISTS Okta_PGCDC_DB.NETWORKS;

-- Create network rule (replace YOUR-POSTGRES-HOST with actual endpoint)
CREATE OR REPLACE NETWORK RULE Okta_PGCDC_DB.NETWORKS.postgres_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('YOUR-POSTGRES-HOST:5432');

-- Create external access integration
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION okta_pgcdc_access
  ALLOWED_NETWORK_RULES = (Okta_PGCDC_DB.NETWORKS.postgres_network_rule)
  ENABLED = TRUE
  COMMENT = 'OpenFlow SPCS runtime access for Okta CDC';

GRANT USAGE ON INTEGRATION okta_pgcdc_access TO ROLE Postgres_HOL_ROLE;
```

## Step 5: Openflow Configuration

### Runtime Configuration
- **Runtime Role**: Use `Postgres_HOL_ROLE` (created in Step 4)

### Destination Parameters
Use the dedicated objects created in Step 4:
| Parameter | Value |
|-----------|-------|
| Destination Database | `Okta_PGCDC_DB` |
| Snowflake Authentication Strategy | `SNOWFLAKE_MANAGED` |
| Snowflake Role | `Postgres_HOL_ROLE` |
| Snowflake Warehouse | `Okta_PGCDC_WH` |

### Schema Creation
- **DO NOT manually create a target schema** - Openflow automatically creates the schema during sync (it will create a `"public"` schema matching the PostgreSQL source)

### Starting the Connector
1. **Enable Services**: Right-click on the postgres connector and select **Enable all controller services**
2. **Start Process Groups**: Right-click on the postgres connector and click **Start**
3. **Monitor Progress**: Watch the connector flow execute the snapshot load

## Case Sensitivity for Synced Objects
PostgreSQL table and column names are synced as lowercase and require double quotes in Snowflake queries:
- Schema: `"public"` (not PUBLIC)
- Tables: `"users"`, `"product_user_assignment"`, `"device_auth_logs"`
- Columns: `"user_id"`, `"account_id"`, `"product_code"`, `"email"`, `"event_timestamp"`, `"auth_event"`

### Verification Query Example
```sql
USE ROLE Postgres_HOL_ROLE;
USE DATABASE Okta_PGCDC_DB;
USE SCHEMA "public";

SHOW TABLES IN SCHEMA Okta_PGCDC_DB."public";

SELECT 'USERS' as table_name, COUNT(*) as row_count FROM "users"
UNION ALL
SELECT 'PRODUCT_USER_ASSIGNMENT', COUNT(*) FROM "product_user_assignment"
UNION ALL
SELECT 'DEVICE_AUTH_LOGS', COUNT(*) FROM "device_auth_logs";
```

## Verification Queries - Proper Quoting
All queries joining PostgreSQL-synced tables must quote schema, table, and column names:

### License Assignment Rate
```sql
SELECT 
    u."account_id",
    a.ACCOUNT_NAME,
    pua."product_code",
    COUNT(DISTINCT pua."user_id") as assigned_users,
    s.QUANTITY as total_licenses,
    ROUND(COUNT(DISTINCT pua."user_id") / s.QUANTITY * 100, 1) as assignment_rate_pct
FROM Okta_PGCDC_DB."public"."product_user_assignment" pua
JOIN Okta_PGCDC_DB."public"."users" u ON pua."user_id" = u."user_id"
JOIN PROD.FINAL.ACCOUNT_DAILY a ON u."account_id" = a.ACCOUNT_ID
JOIN PROD.FINAL.SUBSCRIPTION_DAILY s ON a.ACCOUNT_ID = s.ACCOUNT_ID
JOIN PROD.RAW.SFDC_PRODUCT p ON s.PRODUCT_ID = p.PRODUCT_ID AND p.PRODUCT_CODE = pua."product_code"
GROUP BY u."account_id", a.ACCOUNT_NAME, pua."product_code", s.QUANTITY
ORDER BY assignment_rate_pct DESC;
```

### Feature Adoption by Usage
**Important**: Group by the assigned product (`pua."product_code"`), NOT the auth_event type. This ensures users without auth logs still show under their assigned product with 0 active users, rather than appearing as NULL product rows.

```sql
SELECT 
    u."account_id",
    pua."product_code" as product,
    COUNT(DISTINCT pua."user_id") as assigned_users,
    COUNT(DISTINCT dal."user_id") as active_users,
    ROUND(COUNT(DISTINCT dal."user_id") / COUNT(DISTINCT pua."user_id") * 100, 1) as adoption_rate_pct
FROM Okta_PGCDC_DB."public"."product_user_assignment" pua
JOIN Okta_PGCDC_DB."public"."users" u ON pua."user_id" = u."user_id"
LEFT JOIN Okta_PGCDC_DB."public"."device_auth_logs" dal 
    ON pua."user_id" = dal."user_id" 
    AND pua."product_code" = dal."auth_event":auth_type::VARCHAR
GROUP BY u."account_id", pua."product_code"
ORDER BY adoption_rate_pct;
```

### Auth Event Queries
When querying the JSONB `auth_event` column, quote the column name:
```sql
SELECT 
    "auth_event":auth_type::VARCHAR as product,
    "auth_event":auth_status::VARCHAR as status,
    COUNT(*) as event_count
FROM Okta_PGCDC_DB."public"."device_auth_logs"
GROUP BY "auth_event":auth_type, "auth_event":auth_status;
```

## Troubleshooting Openflow Configuration

### Common `"provided": false` Errors
When exporting Openflow configuration, `"provided": false` indicates a missing parameter value:

1. **PostgreSQL JDBC Driver Not Uploaded**
   - Download `postgresql-42.7.7.jar` from https://jdbc.postgresql.org/download/
   - In Openflow, click on the PostgreSQL JDBC Driver parameter
   - Check "Reference asset" checkbox
   - Click Upload and select the downloaded JAR file

2. **PostgreSQL Password Missing**
   - Click on the PostgreSQL Password parameter
   - Enter the password for the `snowflake_admin` user
   - Click Apply
