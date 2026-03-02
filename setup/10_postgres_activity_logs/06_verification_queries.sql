-- Step 5g & Verification Queries
-- Run these in Snowflake after CDC pipeline is configured

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

-- License Assignment Rate
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

-- Feature Adoption by Usage
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

-- Auth Success Rate by Product
SELECT 
    "auth_event":auth_type::VARCHAR as product,
    "auth_event":auth_status::VARCHAR as status,
    COUNT(*) as event_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY "auth_event":auth_type), 1) as pct
FROM Okta_PGCDC_DB."public"."device_auth_logs"
GROUP BY "auth_event":auth_type, "auth_event":auth_status
ORDER BY product, event_count DESC;

-- Authentication by Device Type and OS
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

-- High Risk Authentication Events
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

-- MFA Method Analysis
SELECT 
    "auth_event":mfa_details:method::VARCHAR as mfa_method,
    "auth_event":mfa_details:provider::VARCHAR as provider,
    "auth_event":auth_status::VARCHAR as status,
    COUNT(*) as event_count
FROM Okta_PGCDC_DB."public"."device_auth_logs"
WHERE "auth_event":auth_type = 'MFA'
GROUP BY 1, 2, 3
ORDER BY mfa_method, event_count DESC;

-- Failed Authentication Analysis
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
