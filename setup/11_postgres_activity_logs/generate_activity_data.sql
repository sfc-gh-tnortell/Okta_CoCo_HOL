-- ============================================================================
-- OpenFlow PostgreSQL CDC - Okta Activity Logs Demo
--
-- This script generates authentication activity data for Okta SSO/MFA products
-- Run this on Snowflake Postgres to generate CDC events that will flow to Snowflake
-- 
-- Data volumes are scaled to match subscription quantities:
-- - Users: 70-100% of license count per account
-- - Product assignments: Users assigned to their purchased products
-- - Auth logs: 60-90% of assigned users have activity (adoption rate)
--
-- Execute from DBeaver, psql, or any Postgres client connected to your instance
-- ============================================================================

-- ============================================================================
-- HELPER: Generate random data
-- ============================================================================

-- First names pool
CREATE TEMP TABLE IF NOT EXISTS first_names (name VARCHAR(50));
TRUNCATE first_names;
INSERT INTO first_names VALUES 
('James'),('Mary'),('John'),('Patricia'),('Robert'),('Jennifer'),('Michael'),('Linda'),
('William'),('Elizabeth'),('David'),('Barbara'),('Richard'),('Susan'),('Joseph'),('Jessica'),
('Thomas'),('Sarah'),('Charles'),('Karen'),('Christopher'),('Lisa'),('Daniel'),('Nancy'),
('Matthew'),('Betty'),('Anthony'),('Margaret'),('Mark'),('Sandra'),('Donald'),('Ashley'),
('Steven'),('Kimberly'),('Paul'),('Emily'),('Andrew'),('Donna'),('Joshua'),('Michelle'),
('Kenneth'),('Carol'),('Kevin'),('Amanda'),('Brian'),('Dorothy'),('George'),('Melissa'),
('Timothy'),('Deborah'),('Ronald'),('Stephanie'),('Edward'),('Rebecca'),('Jason'),('Sharon'),
('Jeffrey'),('Laura'),('Ryan'),('Cynthia'),('Jacob'),('Kathleen'),('Gary'),('Amy'),
('Nicholas'),('Angela'),('Eric'),('Shirley'),('Jonathan'),('Anna'),('Stephen'),('Brenda'),
('Larry'),('Pamela'),('Justin'),('Emma'),('Scott'),('Nicole'),('Brandon'),('Helen'),
('Benjamin'),('Samantha'),('Samuel'),('Katherine'),('Raymond'),('Christine'),('Gregory'),('Debra'),
('Frank'),('Rachel'),('Alexander'),('Carolyn'),('Patrick'),('Janet'),('Jack'),('Catherine');

-- Last names pool
CREATE TEMP TABLE IF NOT EXISTS last_names (name VARCHAR(50));
TRUNCATE last_names;
INSERT INTO last_names VALUES 
('Smith'),('Johnson'),('Williams'),('Brown'),('Jones'),('Garcia'),('Miller'),('Davis'),
('Rodriguez'),('Martinez'),('Hernandez'),('Lopez'),('Gonzalez'),('Wilson'),('Anderson'),('Thomas'),
('Taylor'),('Moore'),('Jackson'),('Martin'),('Lee'),('Perez'),('Thompson'),('White'),
('Harris'),('Sanchez'),('Clark'),('Ramirez'),('Lewis'),('Robinson'),('Walker'),('Young'),
('Allen'),('King'),('Wright'),('Scott'),('Torres'),('Nguyen'),('Hill'),('Flores'),
('Green'),('Adams'),('Nelson'),('Baker'),('Hall'),('Rivera'),('Campbell'),('Mitchell'),
('Carter'),('Roberts'),('Gomez'),('Phillips'),('Evans'),('Turner'),('Diaz'),('Parker'),
('Cruz'),('Edwards'),('Collins'),('Reyes'),('Stewart'),('Morris'),('Morales'),('Murphy'),
('Cook'),('Rogers'),('Gutierrez'),('Ortiz'),('Morgan'),('Cooper'),('Peterson'),('Bailey'),
('Reed'),('Kelly'),('Howard'),('Ramos'),('Kim'),('Cox'),('Ward'),('Richardson'),
('Watson'),('Brooks'),('Chavez'),('Wood'),('James'),('Bennett'),('Gray'),('Mendoza'),
('Ruiz'),('Hughes'),('Price'),('Alvarez'),('Castillo'),('Sanders'),('Patel'),('Myers');

-- Job titles pool
CREATE TEMP TABLE IF NOT EXISTS job_titles (title VARCHAR(100), department VARCHAR(50));
TRUNCATE job_titles;
INSERT INTO job_titles VALUES 
('Software Engineer', 'Engineering'),('Senior Software Engineer', 'Engineering'),('Staff Engineer', 'Engineering'),
('Principal Engineer', 'Engineering'),('Engineering Manager', 'Engineering'),('DevOps Engineer', 'Engineering'),
('Data Engineer', 'Engineering'),('ML Engineer', 'Engineering'),('QA Engineer', 'Engineering'),
('Product Manager', 'Product'),('Senior Product Manager', 'Product'),('Director of Product', 'Product'),
('UX Designer', 'Design'),('Senior UX Designer', 'Design'),('Product Designer', 'Design'),
('Sales Representative', 'Sales'),('Account Executive', 'Sales'),('Sales Manager', 'Sales'),
('Sales Director', 'Sales'),('VP of Sales', 'Sales'),('SDR', 'Sales'),
('Marketing Manager', 'Marketing'),('Content Specialist', 'Marketing'),('Growth Lead', 'Marketing'),
('Financial Analyst', 'Finance'),('Senior Accountant', 'Finance'),('Controller', 'Finance'),
('HR Manager', 'HR'),('Recruiter', 'HR'),('HR Business Partner', 'HR'),('People Ops', 'HR'),
('IT Admin', 'IT'),('IT Manager', 'IT'),('Security Analyst', 'IT'),('Help Desk', 'IT'),
('Operations Manager', 'Operations'),('Operations Analyst', 'Operations'),('Supply Chain Manager', 'Operations'),
('Legal Counsel', 'Legal'),('Compliance Officer', 'Legal'),('Paralegal', 'Legal'),
('Customer Success Manager', 'Customer Success'),('Support Engineer', 'Support'),('Technical Writer', 'Docs'),
('Data Scientist', 'Analytics'),('Business Analyst', 'Analytics'),('BI Developer', 'Analytics');

-- Cities pool with geo data
CREATE TEMP TABLE IF NOT EXISTS cities (city VARCHAR(50), state VARCHAR(50), lat DECIMAL(8,4), lng DECIMAL(9,4), timezone VARCHAR(50));
TRUNCATE cities;
INSERT INTO cities VALUES 
('San Francisco', 'California', 37.7749, -122.4194, 'America/Los_Angeles'),
('New York', 'New York', 40.7128, -74.0060, 'America/New_York'),
('Chicago', 'Illinois', 41.8781, -87.6298, 'America/Chicago'),
('Seattle', 'Washington', 47.6062, -122.3321, 'America/Los_Angeles'),
('Austin', 'Texas', 30.2672, -97.7431, 'America/Chicago'),
('Denver', 'Colorado', 39.7392, -104.9903, 'America/Denver'),
('Boston', 'Massachusetts', 42.3601, -71.0589, 'America/New_York'),
('Atlanta', 'Georgia', 33.7490, -84.3880, 'America/New_York'),
('Los Angeles', 'California', 34.0522, -118.2437, 'America/Los_Angeles'),
('Dallas', 'Texas', 32.7767, -96.7970, 'America/Chicago'),
('Phoenix', 'Arizona', 33.4484, -112.0740, 'America/Phoenix'),
('Portland', 'Oregon', 45.5152, -122.6784, 'America/Los_Angeles'),
('Miami', 'Florida', 25.7617, -80.1918, 'America/New_York'),
('Minneapolis', 'Minnesota', 44.9778, -93.2650, 'America/Chicago'),
('San Diego', 'California', 32.7157, -117.1611, 'America/Los_Angeles');

-- Account data from Snowflake (matches PROD.FINAL tables)
CREATE TEMP TABLE IF NOT EXISTS account_licenses (
    account_id VARCHAR(20), 
    account_name VARCHAR(100), 
    domain VARCHAR(100),
    sso_licenses INT, 
    mfa_licenses INT
);
TRUNCATE account_licenses;
INSERT INTO account_licenses VALUES 
('ACC000001', 'Walmart', 'walmart.com', 4630, NULL),
('ACC000002', 'Amazon', 'amazon.com', 3934, NULL),
('ACC000004', 'Apple', 'apple.com', NULL, 1303),
('ACC000012', 'Chevron', 'chevron.com', 4334, 2739),
('ACC000013', 'Microsoft', 'microsoft.com', 1095, 4715),
('ACC000018', 'JPMorgan Chase', 'jpmchase.com', 4144, NULL),
('ACC000046', 'Boeing', 'boeing.com', 4895, NULL),
('ACC000052', 'Caterpillar', 'caterpillar.com', 1936, 179),
('ACC000088', 'Uber Technologies', 'uber.com', NULL, 4759),
('ACC000095', 'Netflix', 'netflix.com', NULL, 3331),
('ACC000096', '3M', '3m.com', 2217, 1913),
('ACC000150', 'ServiceNow', 'servicenow.com', 4961, NULL),
('ACC000152', 'Workday', 'workday.com', NULL, 3728),
('ACC000159', 'CrowdStrike', 'crowdstrike.com', 3632, NULL),
('ACC000013', 'Microsoft', 'microsoft.com', 1095, 4715);

SELECT pg_sleep(1);

-- ============================================================================
-- STEP 1: Generate Users (70-100% of license count per account)
-- ============================================================================

-- \echo 'Generating users for all accounts...'

DO $$
DECLARE
    acct RECORD;
    user_count INT;
    license_base INT;
    i INT;
    fname VARCHAR(50);
    lname VARCHAR(50);
    jtitle VARCHAR(100);
    dept VARCHAR(50);
    emp_prefix VARCHAR(10);
BEGIN
    FOR acct IN SELECT DISTINCT account_id, account_name, domain, 
                       COALESCE(sso_licenses, 0) + COALESCE(mfa_licenses, 0) as total_licenses
                FROM account_licenses 
                WHERE COALESCE(sso_licenses, 0) + COALESCE(mfa_licenses, 0) > 0
    LOOP
        -- Scale down for demo: use 1-5% of actual license count, min 50, max 500
        license_base := GREATEST(50, LEAST(500, (acct.total_licenses * 0.03)::INT));
        user_count := (license_base * (0.7 + RANDOM() * 0.3))::INT;
        
        emp_prefix := UPPER(SUBSTRING(acct.account_name, 1, 3));
        
        FOR i IN 1..user_count LOOP
            SELECT name INTO fname FROM first_names ORDER BY RANDOM() LIMIT 1;
            SELECT name INTO lname FROM last_names ORDER BY RANDOM() LIMIT 1;
            SELECT title, department INTO jtitle, dept FROM job_titles ORDER BY RANDOM() LIMIT 1;
            
            INSERT INTO users (account_id, email, first_name, last_name, job_title, department, employee_id, is_active, created_at)
            VALUES (
                acct.account_id,
                LOWER(fname || '.' || lname || i || '@' || acct.domain),
                fname,
                lname,
                jtitle,
                dept,
                emp_prefix || LPAD(i::TEXT, 5, '0'),
                RANDOM() > 0.05,  -- 95% active
                NOW() - (RANDOM() * INTERVAL '365 days')
            )
            ON CONFLICT (email) DO NOTHING;
        END LOOP;
        
        RAISE NOTICE 'Created % users for %', user_count, acct.account_name;
    END LOOP;
END $$;

SELECT pg_sleep(2);

-- ============================================================================
-- STEP 2: Assign Product Licenses
-- ============================================================================

-- \echo 'Assigning product licenses to users...'

-- SSO Assignments - for accounts with SSO licenses
INSERT INTO product_user_assignment (user_id, product_code, assigned_date, expiration_date, assignment_status, assigned_by)
SELECT 
    u.user_id, 
    'SSO', 
    (NOW() - (RANDOM() * INTERVAL '180 days'))::DATE,
    (NOW() + (30 + RANDOM() * 335) * INTERVAL '1 day')::DATE,
    CASE WHEN RANDOM() < 0.95 THEN 'active' ELSE 'revoked' END,
    'IT Admin'
FROM users u
JOIN account_licenses al ON u.account_id = al.account_id
WHERE al.sso_licenses IS NOT NULL
  AND RANDOM() < 0.85  -- 85% of users get assigned
ON CONFLICT (user_id, product_code) DO NOTHING;

-- MFA Assignments - for accounts with MFA licenses  
INSERT INTO product_user_assignment (user_id, product_code, assigned_date, expiration_date, assignment_status, assigned_by)
SELECT 
    u.user_id, 
    'MFA', 
    (NOW() - (RANDOM() * INTERVAL '180 days'))::DATE,
    (NOW() + (30 + RANDOM() * 335) * INTERVAL '1 day')::DATE,
    CASE WHEN RANDOM() < 0.95 THEN 'active' ELSE 'revoked' END,
    'IT Admin'
FROM users u
JOIN account_licenses al ON u.account_id = al.account_id
WHERE al.mfa_licenses IS NOT NULL
  AND RANDOM() < 0.80  -- 80% of users get assigned
ON CONFLICT (user_id, product_code) DO NOTHING;

SELECT pg_sleep(2);

-- ============================================================================
-- STEP 3: Generate Authentication Logs
-- ============================================================================

-- \echo 'Generating SSO authentication logs...'

-- SSO Success - Mac/Chrome (corporate)
INSERT INTO device_auth_logs (user_id, event_timestamp, auth_event)
SELECT 
    pua.user_id,
    NOW() - (RANDOM() * INTERVAL '30 days') - (RANDOM() * INTERVAL '24 hours'),
    jsonb_build_object(
        'auth_type', 'SSO',
        'auth_status', 'success',
        'device', jsonb_build_object(
            'type', 'Mac',
            'manufacturer', 'Apple',
            'model', (ARRAY['MacBook Pro 16 (2023)', 'MacBook Pro 14 (2023)', 'MacBook Air M2', 'iMac 24'])[1 + (RANDOM()*3)::INT],
            'os', 'macOS',
            'os_version', (ARRAY['14.4.1 Sonoma', '14.3 Sonoma', '13.6 Ventura', '14.2 Sonoma'])[1 + (RANDOM()*3)::INT],
            'browser', 'Chrome',
            'browser_version', '122.0.' || (6000 + (RANDOM()*500)::INT)::TEXT || '.94'
        ),
        'network', jsonb_build_object(
            'ip_address', (10 + (RANDOM()*240)::INT)::TEXT || '.' || (RANDOM()*255)::INT::TEXT || '.' || (RANDOM()*255)::INT::TEXT || '.' || (1 + (RANDOM()*254)::INT)::TEXT,
            'ip_type', 'corporate',
            'isp', 'Corporate Network'
        ),
        'geo_location', (SELECT jsonb_build_object('city', city, 'state', state, 'country', 'United States', 'country_code', 'US', 'latitude', lat + (RANDOM()-0.5)*0.1, 'longitude', lng + (RANDOM()-0.5)*0.1, 'timezone', timezone) FROM cities ORDER BY RANDOM() LIMIT 1),
        'session', jsonb_build_object(
            'session_id', 'sess_' || substr(md5(random()::text), 1, 12),
            'is_new_device', false,
            'risk_score', (RANDOM() * 20)::INT,
            'risk_factors', '[]'::jsonb
        )
    )
FROM product_user_assignment pua
WHERE pua.product_code = 'SSO' 
  AND pua.assignment_status = 'active'
  AND RANDOM() < 0.75;  -- 75% adoption

SELECT pg_sleep(1);

-- SSO Success - iPhone/Safari (mobile)
INSERT INTO device_auth_logs (user_id, event_timestamp, auth_event)
SELECT 
    pua.user_id,
    NOW() - (RANDOM() * INTERVAL '25 days') - (RANDOM() * INTERVAL '24 hours'),
    jsonb_build_object(
        'auth_type', 'SSO',
        'auth_status', 'success',
        'device', jsonb_build_object(
            'type', 'iPhone',
            'manufacturer', 'Apple',
            'model', (ARRAY['iPhone 15 Pro', 'iPhone 15 Pro Max', 'iPhone 14 Pro', 'iPhone 15', 'iPhone 14'])[1 + (RANDOM()*4)::INT],
            'os', 'iOS',
            'os_version', (ARRAY['17.4.1', '17.4', '17.3.1', '17.3', '16.7.5'])[1 + (RANDOM()*4)::INT],
            'browser', 'Mobile Safari',
            'browser_version', '17.' || (RANDOM()*4)::INT::TEXT
        ),
        'network', jsonb_build_object(
            'ip_address', (10 + (RANDOM()*240)::INT)::TEXT || '.' || (RANDOM()*255)::INT::TEXT || '.' || (RANDOM()*255)::INT::TEXT || '.' || (1 + (RANDOM()*254)::INT)::TEXT,
            'ip_type', (ARRAY['mobile', 'mobile', 'residential', 'corporate'])[1 + (RANDOM()*3)::INT],
            'isp', (ARRAY['Verizon', 'AT&T', 'T-Mobile', 'Corporate Network'])[1 + (RANDOM()*3)::INT]
        ),
        'geo_location', (SELECT jsonb_build_object('city', city, 'state', state, 'country', 'United States', 'country_code', 'US', 'latitude', lat + (RANDOM()-0.5)*0.1, 'longitude', lng + (RANDOM()-0.5)*0.1, 'timezone', timezone) FROM cities ORDER BY RANDOM() LIMIT 1),
        'session', jsonb_build_object(
            'session_id', 'sess_' || substr(md5(random()::text), 1, 12),
            'is_new_device', RANDOM() < 0.1,
            'risk_score', (RANDOM() * 30)::INT,
            'risk_factors', '[]'::jsonb
        )
    )
FROM product_user_assignment pua
WHERE pua.product_code = 'SSO' 
  AND pua.assignment_status = 'active'
  AND RANDOM() < 0.5;

SELECT pg_sleep(1);

-- SSO Success - Windows/Edge (corporate)
INSERT INTO device_auth_logs (user_id, event_timestamp, auth_event)
SELECT 
    pua.user_id,
    NOW() - (RANDOM() * INTERVAL '20 days') - (RANDOM() * INTERVAL '24 hours'),
    jsonb_build_object(
        'auth_type', 'SSO',
        'auth_status', 'success',
        'device', jsonb_build_object(
            'type', 'Windows PC',
            'manufacturer', (ARRAY['Dell', 'HP', 'Lenovo', 'Microsoft'])[1 + (RANDOM()*3)::INT],
            'model', (ARRAY['Latitude 5540', 'EliteBook 840', 'ThinkPad T14s', 'Surface Pro 9'])[1 + (RANDOM()*3)::INT],
            'os', 'Windows',
            'os_version', (ARRAY['11 23H2', '11 22H2', '10 22H2', '11 21H2'])[1 + (RANDOM()*3)::INT],
            'browser', (ARRAY['Edge', 'Chrome', 'Firefox'])[1 + (RANDOM()*2)::INT],
            'browser_version', '122.0.' || (2300 + (RANDOM()*100)::INT)::TEXT || '.80'
        ),
        'network', jsonb_build_object(
            'ip_address', (10 + (RANDOM()*240)::INT)::TEXT || '.' || (RANDOM()*255)::INT::TEXT || '.' || (RANDOM()*255)::INT::TEXT || '.' || (1 + (RANDOM()*254)::INT)::TEXT,
            'ip_type', (ARRAY['corporate', 'corporate', 'residential', 'vpn'])[1 + (RANDOM()*3)::INT],
            'isp', (ARRAY['Corporate Network', 'Comcast', 'Spectrum', 'AT&T'])[1 + (RANDOM()*3)::INT]
        ),
        'geo_location', (SELECT jsonb_build_object('city', city, 'state', state, 'country', 'United States', 'country_code', 'US', 'latitude', lat + (RANDOM()-0.5)*0.1, 'longitude', lng + (RANDOM()-0.5)*0.1, 'timezone', timezone) FROM cities ORDER BY RANDOM() LIMIT 1),
        'session', jsonb_build_object(
            'session_id', 'sess_' || substr(md5(random()::text), 1, 12),
            'is_new_device', false,
            'risk_score', (RANDOM() * 15)::INT,
            'risk_factors', '[]'::jsonb
        )
    )
FROM product_user_assignment pua
WHERE pua.product_code = 'SSO' 
  AND pua.assignment_status = 'active'
  AND RANDOM() < 0.6;

SELECT pg_sleep(1);

-- \echo 'Generating MFA authentication logs...'

-- MFA Success - Push notification (Okta Verify)
INSERT INTO device_auth_logs (user_id, event_timestamp, auth_event)
SELECT 
    pua.user_id,
    NOW() - (RANDOM() * INTERVAL '30 days') - (RANDOM() * INTERVAL '24 hours'),
    jsonb_build_object(
        'auth_type', 'MFA',
        'auth_status', 'success',
        'device', jsonb_build_object(
            'type', (ARRAY['iPhone', 'iPhone', 'Android Phone'])[1 + (RANDOM()*2)::INT],
            'manufacturer', (ARRAY['Apple', 'Apple', 'Samsung', 'Google'])[1 + (RANDOM()*3)::INT],
            'model', (ARRAY['iPhone 15 Pro', 'iPhone 14 Pro', 'Galaxy S24 Ultra', 'Pixel 8 Pro'])[1 + (RANDOM()*3)::INT],
            'os', (ARRAY['iOS', 'iOS', 'Android'])[1 + (RANDOM()*2)::INT],
            'os_version', (ARRAY['17.4.1', '17.3', '14', '14'])[1 + (RANDOM()*3)::INT],
            'browser', 'Okta Verify',
            'browser_version', '9.' || (7 + (RANDOM()*3)::INT)::TEXT || '.0'
        ),
        'network', jsonb_build_object(
            'ip_address', (10 + (RANDOM()*240)::INT)::TEXT || '.' || (RANDOM()*255)::INT::TEXT || '.' || (RANDOM()*255)::INT::TEXT || '.' || (1 + (RANDOM()*254)::INT)::TEXT,
            'ip_type', (ARRAY['corporate', 'mobile', 'residential'])[1 + (RANDOM()*2)::INT],
            'isp', (ARRAY['Corporate Network', 'Verizon', 'AT&T', 'T-Mobile'])[1 + (RANDOM()*3)::INT]
        ),
        'geo_location', (SELECT jsonb_build_object('city', city, 'state', state, 'country', 'United States', 'country_code', 'US', 'latitude', lat + (RANDOM()-0.5)*0.1, 'longitude', lng + (RANDOM()-0.5)*0.1, 'timezone', timezone) FROM cities ORDER BY RANDOM() LIMIT 1),
        'session', jsonb_build_object(
            'session_id', 'sess_' || substr(md5(random()::text), 1, 12),
            'is_new_device', false,
            'risk_score', (RANDOM() * 15)::INT,
            'risk_factors', '[]'::jsonb
        ),
        'mfa_details', jsonb_build_object(
            'method', 'push',
            'provider', 'Okta Verify',
            'challenge_type', (ARRAY['number_match', 'approve_deny'])[1 + (RANDOM()*1)::INT]
        )
    )
FROM product_user_assignment pua
WHERE pua.product_code = 'MFA' 
  AND pua.assignment_status = 'active'
  AND RANDOM() < 0.70;  -- 70% adoption

SELECT pg_sleep(1);

-- MFA Success - TOTP (Google Authenticator)
INSERT INTO device_auth_logs (user_id, event_timestamp, auth_event)
SELECT 
    pua.user_id,
    NOW() - (RANDOM() * INTERVAL '28 days') - (RANDOM() * INTERVAL '24 hours'),
    jsonb_build_object(
        'auth_type', 'MFA',
        'auth_status', 'success',
        'device', jsonb_build_object(
            'type', (ARRAY['Android Phone', 'iPhone'])[1 + (RANDOM()*1)::INT],
            'manufacturer', (ARRAY['Samsung', 'Google', 'Apple'])[1 + (RANDOM()*2)::INT],
            'model', (ARRAY['Galaxy S24 Ultra', 'Pixel 8 Pro', 'iPhone 15'])[1 + (RANDOM()*2)::INT],
            'os', (ARRAY['Android', 'Android', 'iOS'])[1 + (RANDOM()*2)::INT],
            'os_version', (ARRAY['14', '13', '17.4'])[1 + (RANDOM()*2)::INT],
            'browser', 'Google Authenticator',
            'browser_version', '6.0'
        ),
        'network', jsonb_build_object(
            'ip_address', (10 + (RANDOM()*240)::INT)::TEXT || '.' || (RANDOM()*255)::INT::TEXT || '.' || (RANDOM()*255)::INT::TEXT || '.' || (1 + (RANDOM()*254)::INT)::TEXT,
            'ip_type', (ARRAY['mobile', 'residential', 'corporate'])[1 + (RANDOM()*2)::INT],
            'isp', (ARRAY['AT&T', 'Verizon', 'T-Mobile', 'Comcast'])[1 + (RANDOM()*3)::INT]
        ),
        'geo_location', (SELECT jsonb_build_object('city', city, 'state', state, 'country', 'United States', 'country_code', 'US', 'latitude', lat + (RANDOM()-0.5)*0.1, 'longitude', lng + (RANDOM()-0.5)*0.1, 'timezone', timezone) FROM cities ORDER BY RANDOM() LIMIT 1),
        'session', jsonb_build_object(
            'session_id', 'sess_' || substr(md5(random()::text), 1, 12),
            'is_new_device', false,
            'risk_score', (RANDOM() * 20)::INT,
            'risk_factors', '[]'::jsonb
        ),
        'mfa_details', jsonb_build_object(
            'method', 'totp',
            'provider', 'Google Authenticator',
            'challenge_type', 'code_entry'
        )
    )
FROM product_user_assignment pua
WHERE pua.product_code = 'MFA' 
  AND pua.assignment_status = 'active'
  AND RANDOM() < 0.35;

SELECT pg_sleep(1);

-- \echo 'Generating additional auth events for active users...'

-- More SSO events for highly active users (multiple logins)
INSERT INTO device_auth_logs (user_id, event_timestamp, auth_event)
SELECT 
    pua.user_id,
    NOW() - (RANDOM() * INTERVAL '14 days') - (RANDOM() * INTERVAL '24 hours'),
    jsonb_build_object(
        'auth_type', 'SSO',
        'auth_status', 'success',
        'device', jsonb_build_object(
            'type', (ARRAY['Mac', 'Windows PC', 'iPhone'])[1 + (RANDOM()*2)::INT],
            'manufacturer', (ARRAY['Apple', 'Dell', 'Apple'])[1 + (RANDOM()*2)::INT],
            'model', (ARRAY['MacBook Pro 16', 'Latitude 5540', 'iPhone 15 Pro'])[1 + (RANDOM()*2)::INT],
            'os', (ARRAY['macOS', 'Windows', 'iOS'])[1 + (RANDOM()*2)::INT],
            'os_version', (ARRAY['14.4.1', '11 23H2', '17.4.1'])[1 + (RANDOM()*2)::INT],
            'browser', (ARRAY['Chrome', 'Edge', 'Safari'])[1 + (RANDOM()*2)::INT],
            'browser_version', '122.0.6261.94'
        ),
        'network', jsonb_build_object(
            'ip_address', (10 + (RANDOM()*240)::INT)::TEXT || '.' || (RANDOM()*255)::INT::TEXT || '.' || (RANDOM()*255)::INT::TEXT || '.' || (1 + (RANDOM()*254)::INT)::TEXT,
            'ip_type', 'corporate',
            'isp', 'Corporate Network'
        ),
        'geo_location', (SELECT jsonb_build_object('city', city, 'state', state, 'country', 'United States', 'country_code', 'US', 'latitude', lat, 'longitude', lng, 'timezone', timezone) FROM cities ORDER BY RANDOM() LIMIT 1),
        'session', jsonb_build_object(
            'session_id', 'sess_' || substr(md5(random()::text), 1, 12),
            'is_new_device', false,
            'risk_score', (RANDOM() * 10)::INT,
            'risk_factors', '[]'::jsonb
        )
    )
FROM product_user_assignment pua
WHERE pua.product_code = 'SSO' 
  AND pua.assignment_status = 'active'
  AND RANDOM() < 0.4;

SELECT pg_sleep(1);

-- More MFA events for active users
INSERT INTO device_auth_logs (user_id, event_timestamp, auth_event)
SELECT 
    pua.user_id,
    NOW() - (RANDOM() * INTERVAL '21 days') - (RANDOM() * INTERVAL '24 hours'),
    jsonb_build_object(
        'auth_type', 'MFA',
        'auth_status', 'success',
        'device', jsonb_build_object(
            'type', 'iPhone',
            'manufacturer', 'Apple',
            'model', 'iPhone 15 Pro',
            'os', 'iOS',
            'os_version', '17.4.1',
            'browser', 'Okta Verify',
            'browser_version', '9.8.0'
        ),
        'network', jsonb_build_object(
            'ip_address', (10 + (RANDOM()*240)::INT)::TEXT || '.' || (RANDOM()*255)::INT::TEXT || '.' || (RANDOM()*255)::INT::TEXT || '.' || (1 + (RANDOM()*254)::INT)::TEXT,
            'ip_type', 'corporate',
            'isp', 'Corporate Network'
        ),
        'geo_location', (SELECT jsonb_build_object('city', city, 'state', state, 'country', 'United States', 'country_code', 'US', 'latitude', lat, 'longitude', lng, 'timezone', timezone) FROM cities ORDER BY RANDOM() LIMIT 1),
        'session', jsonb_build_object(
            'session_id', 'sess_' || substr(md5(random()::text), 1, 12),
            'is_new_device', false,
            'risk_score', (RANDOM() * 10)::INT,
            'risk_factors', '[]'::jsonb
        ),
        'mfa_details', jsonb_build_object(
            'method', 'push',
            'provider', 'Okta Verify',
            'challenge_type', 'number_match'
        )
    )
FROM product_user_assignment pua
WHERE pua.product_code = 'MFA' 
  AND pua.assignment_status = 'active'
  AND RANDOM() < 0.45;

SELECT pg_sleep(1);

-- \echo 'Generating failed authentication attempts...'

-- SSO Failures (invalid password)
INSERT INTO device_auth_logs (user_id, event_timestamp, auth_event)
SELECT 
    pua.user_id,
    NOW() - (RANDOM() * INTERVAL '15 days') - (RANDOM() * INTERVAL '24 hours'),
    jsonb_build_object(
        'auth_type', 'SSO',
        'auth_status', 'failure',
        'device', jsonb_build_object(
            'type', (ARRAY['Windows PC', 'Mac', 'Linux Workstation'])[1 + (RANDOM()*2)::INT],
            'manufacturer', (ARRAY['HP', 'Dell', 'Lenovo'])[1 + (RANDOM()*2)::INT],
            'model', (ARRAY['EliteBook 840', 'Latitude 5540', 'ThinkPad T14s'])[1 + (RANDOM()*2)::INT],
            'os', (ARRAY['Windows', 'macOS', 'Linux'])[1 + (RANDOM()*2)::INT],
            'os_version', (ARRAY['10 22H2', '13.6', 'Ubuntu 22.04'])[1 + (RANDOM()*2)::INT],
            'browser', (ARRAY['Firefox', 'Chrome', 'Edge'])[1 + (RANDOM()*2)::INT],
            'browser_version', '123.0'
        ),
        'network', jsonb_build_object(
            'ip_address', (10 + (RANDOM()*240)::INT)::TEXT || '.' || (RANDOM()*255)::INT::TEXT || '.' || (RANDOM()*255)::INT::TEXT || '.' || (1 + (RANDOM()*254)::INT)::TEXT,
            'ip_type', (ARRAY['vpn', 'residential', 'mobile'])[1 + (RANDOM()*2)::INT],
            'isp', (ARRAY['NordVPN', 'ExpressVPN', 'Comcast', 'Spectrum'])[1 + (RANDOM()*3)::INT]
        ),
        'geo_location', (SELECT jsonb_build_object('city', city, 'state', state, 'country', 'United States', 'country_code', 'US', 'latitude', lat + (RANDOM()-0.5)*0.1, 'longitude', lng + (RANDOM()-0.5)*0.1, 'timezone', timezone) FROM cities ORDER BY RANDOM() LIMIT 1),
        'session', jsonb_build_object(
            'session_id', 'sess_' || substr(md5(random()::text), 1, 12),
            'is_new_device', RANDOM() < 0.4,
            'risk_score', 50 + (RANDOM() * 50)::INT,
            'risk_factors', (ARRAY['["new_device"]', '["vpn"]', '["new_device", "vpn"]', '["unusual_time"]'])[1 + (RANDOM()*3)::INT]::jsonb
        ),
        'failure_details', jsonb_build_object(
            'reason', (ARRAY['invalid_password', 'account_locked', 'expired_password', 'invalid_username'])[1 + (RANDOM()*3)::INT],
            'attempt_count', 1 + (RANDOM() * 4)::INT,
            'locked_out', RANDOM() < 0.15
        )
    )
FROM product_user_assignment pua
WHERE pua.product_code = 'SSO' 
  AND pua.assignment_status = 'active'
  AND RANDOM() < 0.12;

SELECT pg_sleep(1);

-- MFA Failures (timeout, wrong code)
INSERT INTO device_auth_logs (user_id, event_timestamp, auth_event)
SELECT 
    pua.user_id,
    NOW() - (RANDOM() * INTERVAL '20 days') - (RANDOM() * INTERVAL '24 hours'),
    jsonb_build_object(
        'auth_type', 'MFA',
        'auth_status', 'failure',
        'device', jsonb_build_object(
            'type', (ARRAY['iPhone', 'Android Phone', 'Mac'])[1 + (RANDOM()*2)::INT],
            'manufacturer', (ARRAY['Apple', 'Samsung', 'Apple'])[1 + (RANDOM()*2)::INT],
            'model', (ARRAY['iPhone 14', 'Galaxy S23', 'MacBook Air'])[1 + (RANDOM()*2)::INT],
            'os', (ARRAY['iOS', 'Android', 'macOS'])[1 + (RANDOM()*2)::INT],
            'os_version', (ARRAY['17.3', '14', '13.6'])[1 + (RANDOM()*2)::INT],
            'browser', (ARRAY['Okta Verify', 'Google Authenticator', 'Safari'])[1 + (RANDOM()*2)::INT],
            'browser_version', '9.7.0'
        ),
        'network', jsonb_build_object(
            'ip_address', (10 + (RANDOM()*240)::INT)::TEXT || '.' || (RANDOM()*255)::INT::TEXT || '.' || (RANDOM()*255)::INT::TEXT || '.' || (1 + (RANDOM()*254)::INT)::TEXT,
            'ip_type', (ARRAY['residential', 'mobile', 'corporate'])[1 + (RANDOM()*2)::INT],
            'isp', (ARRAY['Comcast', 'Spectrum', 'Verizon', 'AT&T'])[1 + (RANDOM()*3)::INT]
        ),
        'geo_location', (SELECT jsonb_build_object('city', city, 'state', state, 'country', 'United States', 'country_code', 'US', 'latitude', lat + (RANDOM()-0.5)*0.1, 'longitude', lng + (RANDOM()-0.5)*0.1, 'timezone', timezone) FROM cities ORDER BY RANDOM() LIMIT 1),
        'session', jsonb_build_object(
            'session_id', 'sess_' || substr(md5(random()::text), 1, 12),
            'is_new_device', false,
            'risk_score', 40 + (RANDOM() * 40)::INT,
            'risk_factors', (ARRAY['["unusual_time"]', '[]', '["new_location"]'])[1 + (RANDOM()*2)::INT]::jsonb
        ),
        'mfa_details', jsonb_build_object(
            'method', (ARRAY['push', 'totp', 'sms'])[1 + (RANDOM()*2)::INT],
            'provider', (ARRAY['Okta Verify', 'Google Authenticator', 'SMS'])[1 + (RANDOM()*2)::INT],
            'challenge_type', (ARRAY['number_match', 'code_entry', 'approve_deny'])[1 + (RANDOM()*2)::INT]
        ),
        'failure_details', jsonb_build_object(
            'reason', (ARRAY['mfa_timeout', 'invalid_code', 'push_denied', 'device_not_registered'])[1 + (RANDOM()*3)::INT],
            'attempt_count', 1 + (RANDOM() * 2)::INT,
            'locked_out', false
        )
    )
FROM product_user_assignment pua
WHERE pua.product_code = 'MFA' 
  AND pua.assignment_status = 'active'
  AND RANDOM() < 0.08;

SELECT pg_sleep(1);

-- Challenge/Step-up auth events (high risk requiring additional verification)
INSERT INTO device_auth_logs (user_id, event_timestamp, auth_event)
SELECT 
    pua.user_id,
    NOW() - (RANDOM() * INTERVAL '10 days') - (RANDOM() * INTERVAL '24 hours'),
    jsonb_build_object(
        'auth_type', pua.product_code,
        'auth_status', 'challenge',
        'device', jsonb_build_object(
            'type', (ARRAY['Linux Workstation', 'Windows PC', 'Android Phone'])[1 + (RANDOM()*2)::INT],
            'manufacturer', (ARRAY['Lenovo', 'Dell', 'Samsung'])[1 + (RANDOM()*2)::INT],
            'model', (ARRAY['ThinkPad T14s', 'XPS 15', 'Galaxy S24'])[1 + (RANDOM()*2)::INT],
            'os', (ARRAY['Ubuntu 22.04', 'Windows 11', 'Android 14'])[1 + (RANDOM()*2)::INT],
            'os_version', (ARRAY['22.04 LTS', '23H2', '14'])[1 + (RANDOM()*2)::INT],
            'browser', (ARRAY['Firefox', 'Chrome', 'Chrome Mobile'])[1 + (RANDOM()*2)::INT],
            'browser_version', '122.0'
        ),
        'network', jsonb_build_object(
            'ip_address', (10 + (RANDOM()*240)::INT)::TEXT || '.' || (RANDOM()*255)::INT::TEXT || '.' || (RANDOM()*255)::INT::TEXT || '.' || (1 + (RANDOM()*254)::INT)::TEXT,
            'ip_type', (ARRAY['residential', 'vpn', 'mobile'])[1 + (RANDOM()*2)::INT],
            'isp', (ARRAY['CenturyLink', 'NordVPN', 'T-Mobile'])[1 + (RANDOM()*2)::INT]
        ),
        'geo_location', (SELECT jsonb_build_object('city', city, 'state', state, 'country', 'United States', 'country_code', 'US', 'latitude', lat + (RANDOM()-0.5)*0.2, 'longitude', lng + (RANDOM()-0.5)*0.2, 'timezone', timezone) FROM cities ORDER BY RANDOM() LIMIT 1),
        'session', jsonb_build_object(
            'session_id', 'sess_' || substr(md5(random()::text), 1, 12),
            'is_new_device', true,
            'risk_score', 70 + (RANDOM() * 30)::INT,
            'risk_factors', '["new_device", "new_location"]'::jsonb
        )
    )
FROM product_user_assignment pua
WHERE pua.assignment_status = 'active'
  AND RANDOM() < 0.06;

SELECT pg_sleep(2);

-- ============================================================================
-- SUMMARY: Check Generated Data
-- ============================================================================

-- \echo ''
-- \echo '============================================'
-- \echo 'Activity Log Generation Complete!'
-- \echo '============================================'

SELECT 'Total users' as metric, COUNT(*) as count FROM users
UNION ALL
SELECT 'Total product assignments', COUNT(*) FROM product_user_assignment
UNION ALL
SELECT 'Total auth logs', COUNT(*) FROM device_auth_logs;

-- \echo ''
-- \echo 'Users by company:'
SELECT 
    al.account_name as company,
    COUNT(u.user_id) as user_count,
    COALESCE(al.sso_licenses, 0) as sso_licenses,
    COALESCE(al.mfa_licenses, 0) as mfa_licenses
FROM users u
JOIN account_licenses al ON u.account_id = al.account_id
GROUP BY al.account_name, al.sso_licenses, al.mfa_licenses
ORDER BY user_count DESC;

-- \echo ''
-- \echo 'Product assignment counts:'
SELECT 
    product_code,
    assignment_status,
    COUNT(*) as count
FROM product_user_assignment
GROUP BY product_code, assignment_status
ORDER BY product_code, count DESC;

-- \echo ''
-- \echo 'Auth logs by product and status:'
SELECT 
    auth_event->>'auth_type' as product,
    auth_event->>'auth_status' as status,
    COUNT(*) as count
FROM device_auth_logs
GROUP BY auth_event->>'auth_type', auth_event->>'auth_status'
ORDER BY product, count DESC;

-- \echo ''
-- \echo 'Auth logs by device type:'
SELECT 
    auth_event->'device'->>'type' as device_type,
    COUNT(*) as count
FROM device_auth_logs
GROUP BY auth_event->'device'->>'type'
ORDER BY count DESC;

-- \echo ''
-- \echo 'Adoption rate preview (users with at least one auth log):'
SELECT 
    pua.product_code,
    COUNT(DISTINCT pua.user_id) as assigned_users,
    COUNT(DISTINCT dal.user_id) as active_users,
    ROUND(COUNT(DISTINCT dal.user_id)::DECIMAL / NULLIF(COUNT(DISTINCT pua.user_id), 0) * 100, 1) as adoption_pct
FROM product_user_assignment pua
LEFT JOIN device_auth_logs dal ON pua.user_id = dal.user_id 
    AND pua.product_code = dal.auth_event->>'auth_type'
WHERE pua.assignment_status = 'active'
GROUP BY pua.product_code;

-- \echo ''
-- \echo 'Next Steps:'
-- \echo '1. Start your Openflow CDC pipeline'
-- \echo '2. Data will flow to PROD.ACTIVITY_LOGS schema in Snowflake'
-- \echo '3. Query the data to analyze authentication patterns'
