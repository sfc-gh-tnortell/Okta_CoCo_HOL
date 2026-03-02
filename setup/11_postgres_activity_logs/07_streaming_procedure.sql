-- ============================================================================
-- Streaming Data Simulator for Real-time CDC Testing
-- ============================================================================
-- IMPORTANT: Execute this in your Postgres client (DBeaver, psql CLI, etc.)
--            Do NOT run this in Snowflake UI - this is PostgreSQL code
-- ============================================================================

-- Create procedure to simulate continuous auth log generation
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
    random_ip TEXT;
    random_session_id TEXT;
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
            
            -- Generate random IP address
            random_ip := floor(random() * 256)::int || '.' || 
                         floor(random() * 256)::int || '.' || 
                         floor(random() * 256)::int || '.' || 
                         floor(random() * 256)::int;
            
            -- Generate random session ID
            random_session_id := 'sess_' || substr(md5(random()::text), 1, 12);
            
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
                    'ip_address', random_ip,
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
                    'session_id', random_session_id,
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

-- ============================================================================
-- Usage Examples
-- ============================================================================

-- Generate 100 auth logs (10 batches of 10)
-- CALL stream_auth_logs(10, 10);

-- Generate 500 auth logs (50 batches of 10)
-- CALL stream_auth_logs(10, 50);
