# Okta Activity Logs Pipeline with Snowflake Postgres & Openflow

This guide sets up a pipeline to simulate Okta activity logs using Snowflake Postgres and Openflow CDC streaming into Snowflake tables.

**Goal**: Track product adoption by showing license assignment rates and actual usage through authentication logs.

---

## Prerequisites

- Snowflake account with ACCOUNTADMIN role
- Existing Openflow deployment (`Openflow_Deployment`)
- Completed base setup (accounts, subscriptions, products in PROD schema)

---

## Step 1: Create Network Rule and Postgres Instance

### 1a: Create Network Rule (Required)

Snowflake Postgres requires a network rule to allow external connections. Run this in Snowflake:

```sql
-- Create schema for network objects
USE ROLE SYSADMIN;
CREATE SCHEMA IF NOT EXISTS PROD.NETWORK;

-- Switch to ACCOUNTADMIN for network rule creation
USE ROLE ACCOUNTADMIN;

-- Create network rule allowing all IPs (for demo purposes)
CREATE OR REPLACE NETWORK RULE PROD.NETWORK.POSTGRES_ACCESS_RULE
  TYPE = IPV4
  MODE = POSTGRES_INGRESS
  VALUE_LIST = ('0.0.0.0/0');

-- Create network policy referencing the rule
CREATE OR REPLACE NETWORK POLICY POSTGRES_ACCESS_POLICY
  ALLOWED_NETWORK_RULE_LIST = ('PROD.NETWORK.POSTGRES_ACCESS_RULE');

-- Verify the network rule
SHOW NETWORK RULES IN SCHEMA PROD.NETWORK;
SHOW NETWORK POLICIES;
```

### 1b: Create Postgres Instance via Snowsight

1. Sign in to **Snowsight**
2. Navigate to **Data** > **Databases** > click **+ Database**
3. Select **Postgres** as the database type
4. Configure:
   - **Name**: `okta_activity_logs`
   - **Compute Family**: BURST_S (Burstable, small)
   - **Storage**: 25 GB
   - **Postgres Version**: 18
   - **Network Policy**: Select `POSTGRES_ACCESS_POLICY`

5. Click **Create** and save the connection credentials securely

### 1c: Verify Network Access

After creating the instance, test connectivity:

```bash
# Get the connection string from DESCRIBE POSTGRES INSTANCE output
# Format: postgres://snowflake_admin:<password>@<host>:5432/postgres

# Test connection
psql postgres://snowflake_admin:****@<your-instance>.postgres.snowflake.app:5432/postgres -c "SELECT version();"
```

If connection fails, verify:
1. The network policy is attached to the Postgres instance
2. Port 5432 is not blocked by your firewall

### 1d: Enable Replication for CDC

Openflow CDC requires the database user to have replication privileges. Run this in your Postgres instance:

```sql
-- Enable replication for the admin user (required for Openflow CDC)
ALTER USER snowflake_admin WITH REPLICATION;
```

### 1e: Create Publication for CDC

After creating the tables (Step 2), you must create a publication for CDC replication. Run this in your Postgres instance:

```sql
-- Create publication for all tables in public schema (required for Openflow CDC)
CREATE PUBLICATION openflow_publication FOR TABLES IN SCHEMA public;

-- Verify the publication
SELECT * FROM pg_publication;
```

> **Note**: Run this after Step 2 (Create Postgres Tables) since the publication needs tables to exist.

**Reference**: [Snowflake Postgres Documentation](https://docs.snowflake.com/en/user-guide/snowflake-postgres/about)

---

## Step 2: Create Postgres Tables

Connect to your Postgres instance using psql or a GUI tool:

```bash
psql postgres://snowflake_admin:****@<your-instance>.postgres.snowflake.app:5432/postgres
```

### 2a: Create Users Table

Maps company users to accounts. User count should be 70-100% of subscription license quantity.

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
    employee_id VARCHAR(20),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_users_account ON users(account_id);
CREATE INDEX idx_users_email ON users(email);
```

### 2b: Create Product User Assignment Table

Maps users to licensed products (SSO & MFA focus).

```sql
-- Product assignments: Which users are assigned to which products
CREATE TABLE product_user_assignment (
    assignment_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(user_id),
    product_code VARCHAR(10) NOT NULL,  -- SSO, MFA
    assigned_date DATE NOT NULL DEFAULT CURRENT_DATE,
    expiration_date DATE,
    assignment_status VARCHAR(20) DEFAULT 'active',  -- active, revoked, expired
    assigned_by VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(user_id, product_code)
);

CREATE INDEX idx_assignments_user ON product_user_assignment(user_id);
CREATE INDEX idx_assignments_product ON product_user_assignment(product_code);
CREATE INDEX idx_assignments_status ON product_user_assignment(assignment_status);
```

### 2c: Create Device Authentication Logs Table (Semi-structured JSON)

```sql
-- Authentication logs: Semi-structured device auth events
CREATE TABLE device_auth_logs (
    log_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(user_id),
    event_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    auth_event JSONB NOT NULL
    -- JSONB structure:
    -- {
    --   "auth_type": "SSO" | "MFA",
    --   "auth_status": "success" | "failure" | "denied" | "challenge",
    --   "device": {
    --     "type": "iPhone" | "Android Phone" | "iPad" | "Android Tablet" | "Windows PC" | "Mac" | "Linux",
    --     "manufacturer": "Apple" | "Samsung" | "Google" | "Dell" | "HP" | "Lenovo",
    --     "model": "iPhone 15 Pro" | "Galaxy S24" | "Pixel 8" | "MacBook Pro" | etc.,
    --     "os": "iOS" | "Android" | "Windows" | "macOS" | "Linux",
    --     "os_version": "17.4.1" | "14" | "11" | "14.3" | "Ubuntu 22.04",
    --     "browser": "Chrome" | "Safari" | "Firefox" | "Edge" | "Mobile Safari" | "Chrome Mobile",
    --     "browser_version": "122.0.6261.94"
    --   },
    --   "network": {
    --     "ip_address": "xxx.xxx.xxx.xxx",
    --     "ip_type": "corporate" | "residential" | "mobile" | "vpn",
    --     "isp": "Comcast" | "AT&T" | "Verizon" | etc.
    --   },
    --   "geo_location": {
    --     "city": "San Francisco",
    --     "state": "California",
    --     "country": "United States",
    --     "country_code": "US",
    --     "latitude": 37.7749,
    --     "longitude": -122.4194,
    --     "timezone": "America/Los_Angeles"
    --   },
    --   "mfa_details": {  -- only if auth_type = MFA
    --     "method": "push" | "sms" | "totp" | "biometric" | "hardware_key",
    --     "provider": "Okta Verify" | "Google Authenticator" | "YubiKey",
    --     "challenge_type": "number_match" | "approve_deny" | "code_entry"
    --   },
    --   "session": {
    --     "session_id": "sess_abc123",
    --     "is_new_device": true | false,
    --     "risk_score": 0-100,
    --     "risk_factors": ["new_location", "unusual_time", "impossible_travel"]
    --   },
    --   "failure_details": {  -- only if auth_status = failure/denied
    --     "reason": "invalid_password" | "mfa_timeout" | "device_not_trusted" | "location_blocked",
    --     "attempt_count": 1-5,
    --     "locked_out": false
    --   }
    -- }
);

CREATE INDEX idx_auth_logs_user ON device_auth_logs(user_id);
CREATE INDEX idx_auth_logs_timestamp ON device_auth_logs(event_timestamp);
CREATE INDEX idx_auth_logs_event ON device_auth_logs USING GIN(auth_event);
```

---

## Step 3: Generate Sample Data

Execute the `generate_activity_data.sql` script directly against your Postgres database using DBeaver, psql, or any Postgres client.

### 3a: Optional - Check Account Data in Snowflake

You can run this query in Snowflake to see your account/subscription data:

```sql
-- Run in Snowflake to get SSO/MFA subscription quantities per account
SELECT 
    a.ACCOUNT_ID,
    a.ACCOUNT_NAME,
    p.PRODUCT_CODE,
    s.QUANTITY as LICENSE_COUNT
FROM PROD.FINAL.SUBSCRIPTION_DAILY s
JOIN PROD.FINAL.ACCOUNT_DAILY a ON s.ACCOUNT_ID = a.ACCOUNT_ID
JOIN PROD.RAW.SFDC_PRODUCT p ON s.PRODUCT_ID = p.PRODUCT_ID
WHERE p.PRODUCT_CODE IN ('SSO', 'MFA')
ORDER BY a.ACCOUNT_NAME, p.PRODUCT_CODE;
```

### 3b: Execute the SQL Script

The script uses sequential INSERT statements with `pg_sleep()` between batches for CDC sync timing.

**Option A - Using psql:**
```bash
psql postgres://snowflake_admin:****@<your-instance>.postgres.snowflake.app:5432/postgres \
    -f generate_activity_data.sql
```

**Option B - Using DBeaver or other GUI:**
1. Connect to your Snowflake Postgres instance
2. Open `generate_activity_data.sql`
3. Execute the entire script (F5 or Run button)
4. Watch the progress as data flows through CDC

### 3c: Customize Account IDs (Optional)

The script creates users for accounts `ACC000001`, `ACC000002`, `ACC000003`. Edit the INSERT statements to match your actual account IDs from Snowflake:

```sql
-- Find and replace these account IDs to match your PROD.FINAL.ACCOUNT_DAILY data
INSERT INTO users (account_id, email, ...) VALUES
('YOUR_ACCOUNT_ID', 'user@company.com', ...),
...
```

### 3d: Verify the Data

The script ends with summary queries, but you can also run:

```sql
-- Check record counts
SELECT 'users' as table_name, COUNT(*) as row_count FROM users
UNION ALL
SELECT 'product_user_assignment', COUNT(*) FROM product_user_assignment
UNION ALL
SELECT 'device_auth_logs', COUNT(*) FROM device_auth_logs;

-- Sample auth event structure
SELECT auth_event FROM device_auth_logs LIMIT 1;
```

---

## Step 4: Configure External Access for Openflow

Openflow runs in Snowpark Container Services (SPCS) and needs network access to reach your Postgres instance.

```sql
USE ROLE ACCOUNTADMIN;

-- Step 1: Create Role and Database
-- ----------------------------------------------------------------------------

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

-- Step 2: Create Schema and Network Rules
-- ----------------------------------------------------------------------------

USE ROLE Postgres_HOL_ROLE;
USE DATABASE Okta_PGCDC_DB;

-- Create schema for network rules
CREATE SCHEMA IF NOT EXISTS Okta_PGCDC_DB.NETWORKS;


-- Step 3: Create Network Rules
-- ----------------------------------------------------------------------------
-- IMPORTANT: Replace with your PostgreSQL endpoint
-- 
-- Examples:
-- - GCP Cloud SQL:        '34.123.45.67:5432' (public IP)
-- - AWS RDS:              'mydb.abc123.us-east-1.rds.amazonaws.com:5432'
-- - Azure Database:       'myserver.postgres.database.azure.com:5432'
-- - Self-hosted:          'your-postgres-server.com:5432'

CREATE OR REPLACE NETWORK RULE Okta_PGCDC_DB.NETWORKS.postgres_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('YOUR-POSTGRES-HOST:5432'); -- Replace with your PostgreSQL endpoint

-- Step 4: Create External Access Integration
-- ----------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION okta_pgcdc_access
  ALLOWED_NETWORK_RULES = (
    Okta_PGCDC_DB.NETWORKS.postgres_network_rule
  )
  ENABLED = TRUE
  COMMENT = 'OpenFlow SPCS runtime access for Okta CDC';

-- Grant usage to runtime role
GRANT USAGE ON INTEGRATION okta_pgcdc_access TO ROLE Postgres_HOL_ROLE;
```

---

## Step 4: Configure Openflow CDC Pipeline

### Prerequisites

Before configuring the PostgreSQL connector, ensure you have:
- Completed Step 4 (External Access Integration created)
- An active Openflow deployment (`Openflow_Deployment`)
- Downloaded the PostgreSQL JDBC driver from [https://jdbc.postgresql.org/download/](https://jdbc.postgresql.org/download/) (e.g., `postgresql-42.7.7.jar`)

### 5a: Create Openflow Runtime

1. Navigate to **Data** → **Ingestion** → **Openflow** in Snowsight
2. Click the **Runtimes** tab
3. Ensure your role is set to `ACCOUNTADMIN` or your Openflow admin role
4. Click **+ Runtime** and configure:
   - **Name**: `okta-activity-logs-runtime`
   - **Deployment**: Select `Openflow_Deployment`
   - **Runtime Role**: Select `Postgres_HOL_ROLE` (created in Step 4)
   - **External Access Integration**: Select `okta_pgcdc_access` (created in Step 4)
5. Click **Create** and wait for the runtime status to become **ACTIVE** (3-5 minutes)

### 5b: Add PostgreSQL Connector

1. From the Openflow Overview page, click **+ Add Connector**
2. Select **PostgreSQL** connector
3. Choose your runtime: `okta-activity-logs-runtime`
4. Click to open the runtime canvas

### 5c: Configure PostgreSQL Source Parameters

From the Parameter contexts list, edit **PostgreSQL Source Parameters**:

| Parameter | Value | Description |
|-----------|-------|-------------|
| PostgreSQL Connection URL | `jdbc:postgresql://<your-instance>.postgres.snowflake.app:5432/postgres` | JDBC connection URL |
| PostgreSQL JDBC Driver | `postgresql-42.7.7.jar` | Download from [jdbc.postgresql.org](https://jdbc.postgresql.org/download/), upload as reference asset |
| PostgreSQL Password | `<your-password>` | Password for snowflake_admin user |
| PostgreSQL Username | `snowflake_admin` | PostgreSQL user with REPLICATION privileges |
| Publication Name | `openflow_publication` | The publication created in Step 1e |
| Replication Slot Name | *(leave empty)* | Auto-generated by Openflow |

**To upload the JDBC driver:**
1. Click the file icon next to "PostgreSQL JDBC Driver"
2. Select "Reference Asset"
3. Click "Upload" and select your downloaded `postgresql-42.7.7.jar`
4. Click Apply

### 5d: Configure PostgreSQL Destination Parameters

Edit **PostgreSQL Destination Parameters**:

| Parameter | Value | Description |
|-----------|-------|-------------|
| Destination Database | `Okta_PGCDC_DB` | Target Snowflake database (created in Step 4) |
| Snowflake Authentication Strategy | `SNOWFLAKE_MANAGED` | Uses Snowflake managed authentication |
| Snowflake Role | `Postgres_HOL_ROLE` | Role with table creation privileges (created in Step 4) |
| Snowflake Warehouse | `Okta_PGCDC_WH` | Warehouse for data processing (created in Step 4) |
| Snowflake Account Identifier | *(leave empty)* | Not needed with session token |
| Snowflake Username | *(leave empty)* | Not needed with session token |
| Snowflake Private Key | *(leave empty)* | Not needed with session token |

### 5e: Configure PostgreSQL Ingestion Parameters

Edit **PostgreSQL Ingestion Parameters**:

| Parameter | Value | Description |
|-----------|-------|-------------|
| Included Table Regex | `public\..*` | Matches all tables in public schema |
| Column Filter JSON | `[]` | Empty = include all columns |
| Ingestion Type | `full` | Full snapshot + incremental CDC |
| Merge Task Schedule CRON | `* * * * * ?` | Every second for near real-time |

> **Note**: The regex `public\..*` matches tables like `public.users`, `public.product_user_assignment`, `public.device_auth_logs`. The backslash escapes the dot for literal matching.

### 5f: Enable Services and Start Connector

Before starting the connector, you need to enable the controller services that manage the CDC replication process.

Follow these steps to enable services and start the connector:

1. **Enable Services**: Right-click on the postgres connector and select **Enable all controller services**
2. **Start Process Groups**: Right-click on the postgres connector and click **Start**
3. **Monitor Progress**: Watch the connector flow execute the snapshot load

### 5g: Verify CDC is Working

Check that data is flowing to Snowflake:

```sql
-- Run in Snowflake after a few minutes
USE ROLE Postgres_HOL_ROLE;
USE DATABASE Okta_PGCDC_DB;
USE SCHEMA "public";

-- Check if tables were created
SHOW TABLES IN SCHEMA Okta_PGCDC_DB."public";

-- Verify row counts
SELECT 'USERS' as table_name, COUNT(*) as row_count FROM "users"
UNION ALL
SELECT 'PRODUCT_USER_ASSIGNMENT', COUNT(*) FROM "product_user_assignment"
UNION ALL
SELECT 'DEVICE_AUTH_LOGS', COUNT(*) FROM "device_auth_logs";
```

### Troubleshooting

**Connection Failed?**
- Verify the External Access Integration is enabled: `SHOW EXTERNAL ACCESS INTEGRATIONS;`
- Check the network rule has the correct Postgres endpoint: `DESC NETWORK RULE Okta_PGCDC_DB.NETWORKS.postgres_network_rule;`
- Ensure PostgreSQL allows connections (network policy attached)

**No Data Flowing?**
- Verify the publication exists in PostgreSQL: `SELECT * FROM pg_publication;`
- Check replication slot was created: `SELECT * FROM pg_replication_slots;`
- Verify user has REPLICATION privilege: `SELECT rolname, rolreplication FROM pg_roles WHERE rolname = 'snowflake_admin';`

**Reference**: [Openflow PostgreSQL CDC Documentation](https://docs.snowflake.com/en/user-guide/data-integration/openflow/connectors/postgres/about)

---

## Step 5: Streaming Data Simulator (Optional)

Create a Postgres procedure to simulate continuous auth log generation for real-time CDC testing.

### 6a: Create the Streaming Procedure in Postgres

```sql
-- Execute in Postgres
CREATE OR REPLACE PROCEDURE stream_auth_logs(batch_size INT, num_batches INT)
LANGUAGE plpgsql
AS $$
DECLARE
    i INT;
    j INT;
    active_user RECORD;
    auth_event JSONB;
    device_types TEXT[] := ARRAY['iPhone', 'Android', 'Mac', 'Windows'];
    statuses TEXT[] := ARRAY['success', 'success', 'success', 'failure'];
    device_type TEXT;
    status TEXT;
    total_logs INT := 0;
BEGIN
    FOR i IN 1..num_batches LOOP
        FOR j IN 1..batch_size LOOP
            -- Get random active user
            SELECT u.user_id, pua.product_code INTO active_user
            FROM users u
            JOIN product_user_assignment pua ON u.user_id = pua.user_id
            WHERE pua.assignment_status = 'active'
            ORDER BY random()
            LIMIT 1;
            
            IF active_user IS NULL THEN
                RAISE NOTICE 'No active users found';
                RETURN;
            END IF;
            
            device_type := device_types[1 + floor(random() * 4)::int];
            status := statuses[1 + floor(random() * 4)::int];
            
            auth_event := jsonb_build_object(
                'auth_type', active_user.product_code,
                'auth_status', status,
                'device', jsonb_build_object(
                    'type', device_type,
                    'manufacturer', CASE device_type WHEN 'iPhone' THEN 'Apple' WHEN 'Mac' THEN 'Apple' ELSE 'Samsung' END,
                    'model', CASE device_type WHEN 'iPhone' THEN 'iPhone 15 Pro' WHEN 'Mac' THEN 'MacBook Pro' ELSE 'Galaxy S24' END,
                    'os', CASE device_type WHEN 'iPhone' THEN 'iOS' WHEN 'Mac' THEN 'macOS' WHEN 'Windows' THEN 'Windows' ELSE 'Android' END,
                    'os_version', '17.4',
                    'browser', 'Chrome',
                    'browser_version', '122.0.6261'
                ),
                'network', jsonb_build_object(
                    'ip_address', fake_ipv4(),
                    'ip_type', 'corporate',
                    'isp', 'Corporate Network'
                ),
                'geo_location', jsonb_build_object(
                    'city', 'San Francisco',
                    'state', 'California',
                    'country', 'United States',
                    'country_code', 'US',
                    'latitude', 37.7749,
                    'longitude', -122.4194,
                    'timezone', 'America/Los_Angeles'
                ),
                'session', jsonb_build_object(
                    'session_id', 'sess_' || fake_uuid_short(),
                    'is_new_device', false,
                    'risk_score', floor(random() * 30)::int,
                    'risk_factors', '[]'::jsonb
                )
            );
            
            IF active_user.product_code = 'MFA' THEN
                auth_event := auth_event || jsonb_build_object(
                    'mfa_details', jsonb_build_object('method', 'push', 'provider', 'Okta Verify', 'challenge_type', 'number_match')
                );
            END IF;
            
            INSERT INTO device_auth_logs (user_id, event_timestamp, auth_event)
            VALUES (active_user.user_id, NOW(), auth_event);
            
            total_logs := total_logs + 1;
        END LOOP;
        COMMIT;
    END LOOP;
    
    RAISE NOTICE 'Generated % auth logs in % batches', total_logs, num_batches;
END;
$$;
```

### 6b: Execute Streaming Simulation

```sql
-- Generate 100 auth logs (10 batches of 10)
CALL stream_auth_logs(10, 10);
```

### 6c: Continuous Streaming with pg_cron (Optional)

If you have pg_cron extension enabled:

```sql
-- Schedule to run every minute
SELECT cron.schedule('stream-auth-logs', '* * * * *', 'CALL stream_auth_logs(5, 1)');

-- View scheduled jobs
SELECT * FROM cron.job;

-- Remove the schedule
SELECT cron.unschedule('stream-auth-logs');
```

---

## Verification Queries

After data flows to Snowflake, verify with these queries:

### License Assignment Rate
```sql
-- What % of licenses are assigned to users?
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
```sql
-- Of assigned users, how many are actually using the product?
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

### Auth Success Rate by Product
```sql
-- Authentication success/failure breakdown
SELECT 
    "auth_event":auth_type::VARCHAR as product,
    "auth_event":auth_status::VARCHAR as status,
    COUNT(*) as event_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY "auth_event":auth_type), 1) as pct
FROM Okta_PGCDC_DB."public"."device_auth_logs"
GROUP BY "auth_event":auth_type, "auth_event":auth_status
ORDER BY product, event_count DESC;
```

### Authentication by Device Type and OS
```sql
-- Breakdown by device type and operating system
SELECT 
    "auth_event":device:type::VARCHAR as device_type,
    "auth_event":device:os::VARCHAR as os,
    "auth_event":device:os_version::VARCHAR as os_version,
    COUNT(*) as auth_count,
    SUM(CASE WHEN "auth_event":auth_status = 'success' THEN 1 ELSE 0 END) as success_count,
    ROUND(SUM(CASE WHEN "auth_event":auth_status = 'success' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as success_rate
FROM Okta_PGCDC_DB."public"."device_auth_logs"
GROUP BY 1, 2, 3
ORDER BY auth_count DESC;
```

### Authentication by Geographic Location
```sql
-- Auth events by city and timezone
SELECT 
    "auth_event":geo_location:city::VARCHAR as city,
    "auth_event":geo_location:state::VARCHAR as state,
    "auth_event":geo_location:timezone::VARCHAR as timezone,
    COUNT(*) as auth_count,
    AVG("auth_event":session:risk_score::INT) as avg_risk_score
FROM Okta_PGCDC_DB."public"."device_auth_logs"
GROUP BY 1, 2, 3
ORDER BY auth_count DESC;
```

### High Risk Authentication Events
```sql
-- Events with elevated risk scores or new devices
SELECT 
    u."account_id",
    u."email",
    "auth_event":device:type::VARCHAR as device,
    "auth_event":device:manufacturer::VARCHAR as manufacturer,
    "auth_event":geo_location:city::VARCHAR as city,
    "auth_event":session:risk_score::INT as risk_score,
    "auth_event":session:risk_factors as risk_factors,
    "auth_event":session:is_new_device::BOOLEAN as new_device,
    dal."event_timestamp"
FROM Okta_PGCDC_DB."public"."device_auth_logs" dal
JOIN Okta_PGCDC_DB."public"."users" u ON dal."user_id" = u."user_id"
WHERE "auth_event":session:risk_score::INT > 50
   OR "auth_event":session:is_new_device::BOOLEAN = true
ORDER BY risk_score DESC
LIMIT 100;
```

### MFA Method Analysis
```sql
-- MFA authentication breakdown by method
SELECT 
    "auth_event":mfa_details:method::VARCHAR as mfa_method,
    "auth_event":mfa_details:provider::VARCHAR as provider,
    "auth_event":auth_status::VARCHAR as status,
    COUNT(*) as event_count
FROM Okta_PGCDC_DB."public"."device_auth_logs"
WHERE "auth_event":auth_type = 'MFA'
GROUP BY 1, 2, 3
ORDER BY mfa_method, event_count DESC;
```

### Failed Authentication Analysis
```sql
-- Analyze authentication failures by reason
SELECT 
    "auth_event":failure_details:reason::VARCHAR as failure_reason,
    "auth_event":device:type::VARCHAR as device_type,
    COUNT(*) as failure_count,
    COUNT(DISTINCT dal."user_id") as affected_users,
    SUM(CASE WHEN "auth_event":failure_details:locked_out::BOOLEAN THEN 1 ELSE 0 END) as lockouts
FROM Okta_PGCDC_DB."public"."device_auth_logs" dal
WHERE "auth_event":auth_status IN ('failure', 'denied')
GROUP BY 1, 2
ORDER BY failure_count DESC;
```

---

## Reference Documentation

- [Snowflake Postgres Documentation](https://docs.snowflake.com/en/user-guide/snowflake-postgres/about)
- [Snowflake Postgres Getting Started Guide](https://www.snowflake.com/en/developers/guides/getting-started-with-snowflake-postgres/)
- [Openflow CDC Documentation](https://docs.snowflake.com/en/user-guide/data-load-openflow)
- [PostgreSQL JSONB Documentation](https://www.postgresql.org/docs/current/datatype-json.html)
