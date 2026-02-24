-- ============================================================
-- Step 3g: Generate Gong Call Transcripts
-- ============================================================
-- Creates call transcripts for 25% of customers with business insights
-- about fiscal planning, layoffs, tech changes, etc.

INSERT INTO PROD.RAW.GONG_TRANSCRIPT
WITH selected_accounts AS (
    -- Select 25% of accounts (every 4th account based on hash)
    SELECT 
        ACCOUNT_ID,
        ACCOUNT_NAME,
        INDUSTRY,
        HEALTHSCORE,
        ROW_NUMBER() OVER (ORDER BY ACCOUNT_ID) AS rn
    FROM PROD.RAW.SFDC_ACCOUNT
    WHERE MOD(ABS(HASH(ACCOUNT_ID)), 4) = 0
),
transcript_data AS (
    SELECT 
        a.*,
        -- Generate 1-3 calls per account
        call_num.n AS call_number,
        DATEADD(day, -MOD(ABS(HASH(a.ACCOUNT_ID || call_num.n::VARCHAR)), 180), CURRENT_DATE()) AS call_date
    FROM selected_accounts a
    CROSS JOIN (SELECT 1 AS n UNION SELECT 2 UNION SELECT 3) call_num
    WHERE call_num.n <= 1 + MOD(ABS(HASH(a.ACCOUNT_ID)), 3)
)
SELECT 
    'GONG' || LPAD((rn * 10 + call_number)::VARCHAR, 7, '0') AS transcript_id,
    ACCOUNT_ID,
    ACCOUNT_NAME,
    call_date,
    30 + MOD(ABS(HASH(ACCOUNT_ID || call_number::VARCHAR)), 60) AS call_duration_minutes,
    'AE: ' || 
    CASE MOD(ABS(HASH(ACCOUNT_ID)), 5)
        WHEN 0 THEN 'Sarah Johnson'
        WHEN 1 THEN 'Michael Chen'
        WHEN 2 THEN 'Emily Rodriguez'
        WHEN 3 THEN 'James Williams'
        ELSE 'Amanda Thompson'
    END || ', SE: ' ||
    CASE MOD(ABS(HASH(ACCOUNT_ID || 'se')), 5)
        WHEN 0 THEN 'David Park'
        WHEN 1 THEN 'Lisa Anderson'
        WHEN 2 THEN 'Kevin Martinez'
        WHEN 3 THEN 'Rachel Green'
        ELSE 'Chris Taylor'
    END || ', Customer: VP of IT' AS participants,
    CASE MOD(ABS(HASH(ACCOUNT_ID || call_number::VARCHAR || 'type')), 4)
        WHEN 0 THEN 'Quarterly Business Review'
        WHEN 1 THEN 'Renewal Discussion'
        WHEN 2 THEN 'Expansion Opportunity'
        ELSE 'Technical Deep Dive'
    END AS call_type,
    -- Summary with business insights
    CASE MOD(ABS(HASH(ACCOUNT_ID || call_number::VARCHAR || 'summary')), 12)
        WHEN 0 THEN 'Customer discussed upcoming fiscal year planning. Budget cycles starting in Q4. They mentioned potential headcount reduction of 10-15% due to market conditions. Identity security remains a priority despite cuts.'
        WHEN 1 THEN 'Customer is undergoing significant tech stack consolidation. Moving away from legacy systems. Strong interest in expanding identity capabilities. New CTO starting next month with cloud-first mandate.'
        WHEN 2 THEN 'Discussed recent layoffs impacting IT team. Customer needs to do more with less. Automation and self-service capabilities are now critical requirements. Budget under scrutiny.'
        WHEN 3 THEN 'Customer acquired smaller competitor last quarter. Integration project underway. Need to consolidate identity systems. Potential 2x user count increase. Timeline is aggressive.'
        WHEN 4 THEN 'Major compliance audit coming up in 60 days. Customer stressed about access governance gaps. Needs to demonstrate least privilege enforcement. Budget available for compliance tools.'
        WHEN 5 THEN 'Customer mentioned competitor evaluation for MFA solution. Price sensitivity is high. Our champion is pushing for renewal but procurement is challenging every line item.'
        WHEN 6 THEN 'Board-level security initiative announced. Zero trust architecture mandate from CISO. Customer looking to expand from SSO to full workforce identity suite. Executive sponsorship secured.'
        WHEN 7 THEN 'Customer experiencing rapid growth - 40% headcount increase planned. Current onboarding taking too long. Lifecycle management is top priority. Budget approved for automation.'
        WHEN 8 THEN 'Digital transformation project kicked off. Customer moving to cloud-first. Legacy on-prem apps need to be secured. Access Gateway discussion. 18-month timeline.'
        WHEN 9 THEN 'Customer mentioned hiring freeze but existing contracts are safe. Need to show clear ROI for any expansion. Asked about consumption-based pricing options.'
        WHEN 10 THEN 'Recent security incident at peer company has elevated identity security to board level. Customer fast-tracking privileged access management evaluation. Budget cycle being bypassed.'
        ELSE 'Customer consolidating vendors. Currently using 3 identity solutions. Looking to standardize. Competitive bake-off planned for Q2. Need to demonstrate platform value.'
    END AS summary,
    -- Key insights with specific business intelligence
    CASE MOD(ABS(HASH(ACCOUNT_ID || call_number::VARCHAR || 'insights')), 10)
        WHEN 0 THEN 'FISCAL: Q4 budget planning starts next month. 15% overall IT budget reduction expected. Identity security protected but expansion frozen until FY next year.'
        WHEN 1 THEN 'LAYOFFS: 200 employees being let go next quarter. IT team losing 3 members. Need faster offboarding. Current process takes 2 weeks - unacceptable security risk.'
        WHEN 2 THEN 'TECH CHANGE: Migrating from on-prem AD to cloud identity. 18-month project. Hybrid needed during transition. Competitor Azure AD is free with M365.'
        WHEN 3 THEN 'M&A: Acquiring company with 5,000 employees. Due diligence revealed they use competitor. Integration decision pending. Opportunity to expand or risk of churn.'
        WHEN 4 THEN 'GROWTH: IPO planned for next year. SOX compliance requirements driving identity governance needs. Willing to invest in access certification automation.'
        WHEN 5 THEN 'SECURITY: Ransomware attack at competitor. Board asking about MFA coverage. Currently at 60% - need to reach 100%. Timeline: 90 days.'
        WHEN 6 THEN 'LEADERSHIP: New CISO starting. Previously worked at company using our competitor. Need to win them over quickly. Technical deep dive requested.'
        WHEN 7 THEN 'BUDGET: FY budget approved with 20% increase for security. Identity is top priority. Customer wants comprehensive proposal by end of month.'
        WHEN 8 THEN 'CHURN RISK: Key stakeholder leaving the company. Replacement not yet identified. Need to build relationships with IT Director backup.'
        ELSE 'EXPANSION: Customer wants to add API Access Management and Workflows. POC requested. 500 developer seats initially, scaling to 2,000.'
    END AS key_insights,
    -- Next steps
    CASE MOD(ABS(HASH(ACCOUNT_ID || call_number::VARCHAR || 'next')), 6)
        WHEN 0 THEN 'Schedule follow-up with new CISO in 2 weeks. Prepare competitive displacement playbook.'
        WHEN 1 THEN 'Send ROI calculator and customer reference for similar company. Schedule technical deep dive.'
        WHEN 2 THEN 'Prepare expansion proposal with volume discounts. Include migration services for acquired company.'
        WHEN 3 THEN 'Set up POC environment for API Access Management. Schedule developer workshop.'
        WHEN 4 THEN 'Connect customer with compliance team for audit preparation support. Send governance best practices guide.'
        ELSE 'Schedule executive business review with regional VP. Discuss strategic partnership.'
    END AS next_steps,
    -- Sentiment
    CASE MOD(ABS(HASH(ACCOUNT_ID || call_number::VARCHAR || 'sent')), 5)
        WHEN 0 THEN 'Very Positive'
        WHEN 1 THEN 'Positive'
        WHEN 2 THEN 'Neutral'
        WHEN 3 THEN 'Concerned'
        ELSE 'Negative'
    END AS sentiment,
    -- Full transcript text (abbreviated for demo)
    '[00:00] AE: Thanks for joining today. How are things going at ' || ACCOUNT_NAME || '?

[00:15] Customer: Good to connect. Things have been busy with ' ||
    CASE MOD(ABS(HASH(ACCOUNT_ID || call_number::VARCHAR)), 5)
        WHEN 0 THEN 'the budget planning cycle'
        WHEN 1 THEN 'some organizational changes'
        WHEN 2 THEN 'our digital transformation project'
        WHEN 3 THEN 'the upcoming compliance audit'
        ELSE 'evaluating our security stack'
    END || '.

[02:30] SE: I understand. How is the identity platform performing for your team?

[03:00] Customer: Overall good. We have about ' || (1000 + MOD(ABS(HASH(ACCOUNT_ID)), 9000))::VARCHAR || ' users now. ' ||
    CASE MOD(ABS(HASH(ACCOUNT_ID || 'usage')), 3)
        WHEN 0 THEN 'SSO adoption is at 95% which is great.'
        WHEN 1 THEN 'MFA rollout completed last quarter.'
        ELSE 'Looking to expand to more applications.'
    END || '

[08:00] AE: Thats great to hear. I wanted to discuss ' ||
    CASE MOD(ABS(HASH(ACCOUNT_ID || call_number::VARCHAR || 'topic')), 4)
        WHEN 0 THEN 'your upcoming renewal and any new requirements.'
        WHEN 1 THEN 'some additional capabilities that might help with your initiatives.'
        WHEN 2 THEN 'how we can support your security goals.'
        ELSE 'the roadmap and some exciting new features.'
    END || '

[15:00] Customer: ' ||
    CASE MOD(ABS(HASH(ACCOUNT_ID || call_number::VARCHAR || 'response')), 6)
        WHEN 0 THEN 'We are looking at our budget carefully this year. The CFO is asking every team to justify spend.'
        WHEN 1 THEN 'Actually, we have budget approved for security investments. What do you recommend?'
        WHEN 2 THEN 'There have been some changes. We had layoffs last month that affected the IT team.'
        WHEN 3 THEN 'We acquired a company and need to think about consolidating identity systems.'
        WHEN 4 THEN 'Our new CISO is pushing for zero trust. Identity is central to that.'
        ELSE 'We are evaluating alternatives to ensure we are getting the best value.'
    END || '

[25:00] SE: Let me show you how ' ||
    CASE MOD(ABS(HASH(ACCOUNT_ID || 'demo')), 4)
        WHEN 0 THEN 'Lifecycle Management can automate your onboarding and offboarding workflows.'
        WHEN 1 THEN 'Access Governance can help with your compliance requirements.'
        WHEN 2 THEN 'our API Access Management secures your developer APIs.'
        ELSE 'Privileged Access Management protects your critical infrastructure.'
    END || '

[45:00] Customer: This is helpful. Let me discuss with my team and get back to you.

[46:00] AE: Perfect. I will send over a summary and proposed next steps. Thanks for your time today.

[END OF TRANSCRIPT]' AS transcript_text,
    ACCOUNT_NAME || '_' || TO_CHAR(call_date, 'YYYY-MM-DD') || '_call.txt' AS file_name,
    CURRENT_TIMESTAMP() AS created_date
FROM transcript_data;

-- Verify transcript counts
SELECT 
    sentiment,
    COUNT(*) as count
FROM PROD.RAW.GONG_TRANSCRIPT
GROUP BY sentiment
ORDER BY count DESC;

SELECT COUNT(*) as total_transcripts FROM PROD.RAW.GONG_TRANSCRIPT;
SELECT COUNT(DISTINCT ACCOUNT_ID) as accounts_with_transcripts FROM PROD.RAW.GONG_TRANSCRIPT;
