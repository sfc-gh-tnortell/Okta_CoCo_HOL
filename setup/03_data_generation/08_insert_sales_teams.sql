-- ============================================================
-- Step 3h: Insert Sales Team Data
-- ============================================================

INSERT INTO PROD.RAW.SALES_TEAM (TEAM_ID, TERRITORY, TIMEZONE, REGION, ACCOUNT_EXECUTIVE, SALES_ENGINEER, SDR)
VALUES
    ('TEAM001', 'West', 'Pacific', 'Americas West', 'Jennifer Martinez', 'Kevin Chen', 'Ashley Taylor'),
    ('TEAM002', 'West', 'Pacific', 'Americas West', 'Robert Kim', 'Maria Rodriguez', 'Brandon Lee'),
    ('TEAM003', 'West', 'Pacific', 'Americas West', 'Sarah Johnson', 'David Park', 'Emily Wong'),
    ('TEAM004', 'Mountain', 'Mountain', 'Americas Mountain', 'Michael Brown', 'Lisa Anderson', 'Chris Martinez'),
    ('TEAM005', 'Mountain', 'Mountain', 'Americas Mountain', 'Amanda Davis', 'Jason Thompson', 'Nicole Garcia'),
    ('TEAM006', 'Central', 'Central', 'Americas Central', 'James Wilson', 'Rachel Green', 'Tyler Smith'),
    ('TEAM007', 'Central', 'Central', 'Americas Central', 'Michelle Lee', 'Daniel Harris', 'Samantha Clark'),
    ('TEAM008', 'Central', 'Central', 'Americas Central', 'Andrew Miller', 'Jessica White', 'Ryan Johnson'),
    ('TEAM009', 'East', 'Eastern', 'Americas East', 'Elizabeth Moore', 'Christopher Lee', 'Megan Taylor'),
    ('TEAM010', 'East', 'Eastern', 'Americas East', 'William Taylor', 'Amanda Chen', 'Justin Williams'),
    ('TEAM011', 'East', 'Eastern', 'Americas East', 'Patricia Anderson', 'Steven Rodriguez', 'Lauren Brown');

SELECT * FROM PROD.RAW.SALES_TEAM ORDER BY TEAM_ID;
