-- Step 2: Create Postgres Tables
-- Run these DDL statements in your Snowflake Postgres instance

-- 2a: Users table - Company user profiles
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

-- 2b: Product assignments - Which users are assigned to which products
CREATE TABLE product_user_assignment (
    assignment_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(user_id),
    product_code VARCHAR(10) NOT NULL,
    assigned_date DATE NOT NULL DEFAULT CURRENT_DATE,
    expiration_date DATE,
    assignment_status VARCHAR(20) DEFAULT 'active',
    assigned_by VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(user_id, product_code)
);

CREATE INDEX idx_assignments_user ON product_user_assignment(user_id);
CREATE INDEX idx_assignments_product ON product_user_assignment(product_code);
CREATE INDEX idx_assignments_status ON product_user_assignment(assignment_status);

-- 2c: Authentication logs - Semi-structured device auth events
CREATE TABLE device_auth_logs (
    log_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(user_id),
    event_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    auth_event JSONB NOT NULL
);

CREATE INDEX idx_auth_logs_user ON device_auth_logs(user_id);
CREATE INDEX idx_auth_logs_timestamp ON device_auth_logs(event_timestamp);
CREATE INDEX idx_auth_logs_event ON device_auth_logs USING GIN(auth_event);
