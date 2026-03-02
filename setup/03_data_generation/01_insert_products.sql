-- ============================================================
-- Step 3a: Insert Okta-Style Identity Products
-- ============================================================

USE ROLE SYSADMIN;
USE WAREHOUSE DEFAULT_WH;

INSERT INTO PROD.RAW.SFDC_PRODUCT (
    PRODUCT_ID, PRODUCT_NAME, PRODUCT_CODE, PRODUCT_DESCRIPTION, 
    PRODUCT_FAMILY, PRODUCT_LINE, PRODUCT_CATEGORY, PRODUCT_UNIT, 
    IS_ACTIVE, LIST_PRICE_USD, CREATED_DATE
)
VALUES
    ('PROD001', 'Single Sign-On (SSO)', 'SSO', 'Secure one-click access to all cloud and on-prem apps', 'Identity & Access Management', 'Workforce Identity', 'Core', 'per user/month', TRUE, 6.00, CURRENT_TIMESTAMP()),
    ('PROD002', 'Multi-Factor Authentication (MFA)', 'MFA', 'Basic multi-factor authentication', 'Identity & Access Management', 'Workforce Identity', 'Security', 'per user/month', TRUE, 3.00, CURRENT_TIMESTAMP()),
    ('PROD003', 'Adaptive MFA', 'AMFA', 'Intelligent, phishing-resistant authentication', 'Identity & Access Management', 'Workforce Identity', 'Security', 'per user/month', TRUE, 6.00, CURRENT_TIMESTAMP()),
    ('PROD004', 'Universal Directory', 'UD', 'Centralized unified directory', 'Identity & Access Management', 'Workforce Identity', 'Core', 'per user/month', TRUE, 2.00, CURRENT_TIMESTAMP()),
    ('PROD005', 'Lifecycle Management', 'LCM', 'Automate user onboarding and offboarding', 'Identity Governance', 'Workforce Identity', 'Automation', 'per user/month', TRUE, 8.00, CURRENT_TIMESTAMP()),
    ('PROD006', 'API Access Management', 'API', 'Secure APIs and microservices', 'Identity & Access Management', 'Workforce Identity', 'API', 'per user/month', TRUE, 4.00, CURRENT_TIMESTAMP()),
    ('PROD007', 'Device Access', 'DA', 'Passwordless authentication for desktops', 'Identity & Access Management', 'Workforce Identity', 'Device', 'per user/month', TRUE, 5.00, CURRENT_TIMESTAMP()),
    ('PROD008', 'Access Governance', 'AG', 'Automate access reviews and requests', 'Identity Governance', 'Workforce Identity', 'Governance', 'per user/month', TRUE, 8.00, CURRENT_TIMESTAMP()),
    ('PROD009', 'Privileged Access', 'PAM', 'Govern privileged access to infrastructure', 'Privileged Access Management', 'Workforce Identity', 'Security', 'per user/month', TRUE, 15.00, CURRENT_TIMESTAMP()),
    ('PROD010', 'Workflows', 'WF', 'No-code identity process automation', 'Identity Orchestration', 'Workforce Identity', 'Automation', 'per user/month', TRUE, 4.00, CURRENT_TIMESTAMP()),
    ('PROD011', 'Identity Threat Protection', 'ITP', 'AI-powered identity threat detection', 'Identity Security', 'Workforce Identity', 'Security', 'per user/month', TRUE, 5.00, CURRENT_TIMESTAMP()),
    ('PROD012', 'Identity Security Posture Management', 'ISPM', 'Discover and remediate identity risks', 'Identity Security', 'Workforce Identity', 'Security', 'per user/month', TRUE, 6.00, CURRENT_TIMESTAMP()),
    ('PROD013', 'Access Gateway', 'AGW', 'Secure on-prem apps without code changes', 'Identity & Access Management', 'Workforce Identity', 'On-Prem', 'per user/month', TRUE, 5.00, CURRENT_TIMESTAMP()),
    ('PROD014', 'Secure Partner Access', 'SPA', 'Enable secure partner interactions', 'Identity & Access Management', 'Workforce Identity', 'B2B', 'per user/month', TRUE, 4.00, CURRENT_TIMESTAMP());

SELECT PRODUCT_CODE, PRODUCT_NAME, LIST_PRICE_USD FROM PROD.RAW.SFDC_PRODUCT ORDER BY PRODUCT_ID;
