"""
Generate Gong Call Transcript Files
====================================
Creates text files for 25% of customers with business insights.
Run this after generating account data in Snowflake.

Usage:
    python generate_transcripts.py

Prerequisites:
    pip install snowflake-connector-python
"""

import os
import snowflake.connector
from datetime import datetime, timedelta
import random

# Connect to Snowflake
conn = snowflake.connector.connect(
    connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "sfsenorthamerica-demo351_aws"
)

# Create output directory
output_dir = os.path.join(os.path.dirname(__file__), "..", "gong_transcripts")
os.makedirs(output_dir, exist_ok=True)

# Get accounts for transcript generation (25% of accounts)
cursor = conn.cursor()
cursor.execute("""
    SELECT 
        ACCOUNT_ID,
        ACCOUNT_NAME,
        INDUSTRY,
        HEALTHSCORE,
        BILLING_CITY,
        BILLING_STATE,
        NUMBER_OF_EMPLOYEES,
        CARR_USD
    FROM PROD.RAW.SFDC_ACCOUNT
    WHERE MOD(ABS(HASH(ACCOUNT_ID)), 4) = 0
    ORDER BY ACCOUNT_NAME
""")
accounts = cursor.fetchall()

print(f"Generating transcripts for {len(accounts)} accounts (25% of total)")

# Insight templates
fiscal_insights = [
    "Customer mentioned Q4 budget planning starts next month. Expecting 15% overall IT budget reduction. Identity security protected but expansion frozen until next FY.",
    "CFO mandated 20% cost reduction across all departments. Customer looking to consolidate vendors. We need to show strong ROI to protect our footprint.",
    "Budget approved for security investments! Customer has $500K allocated for identity modernization. Need to move fast before end of quarter.",
    "Customer's fiscal year ends in March. They have remaining budget to use. Opportunity to expand if we can close by February 15th.",
    "IPO planned for next year. SOX compliance requirements driving identity governance investments. Customer willing to invest significantly."
]

layoff_insights = [
    "Customer announced 200 employee layoff next quarter. IT team losing 3 members. Need faster offboarding - current process takes 2 weeks creating security risk.",
    "Hiring freeze in effect but existing contracts are protected. Customer needs to show clear ROI for any expansion. Asked about consumption-based pricing.",
    "Reorganization underway - customer consolidating 3 IT teams into 1. Key stakeholder moving to new role. Need to build relationships with new team lead.",
    "Customer mentioned their competitor just had layoffs. They're concerned about talent acquisition. Lifecycle Management for faster onboarding is appealing.",
    "New CHRO starting next month. Previous one was our champion. Need to quickly establish relationship with incoming executive."
]

tech_insights = [
    "Customer migrating from on-prem AD to cloud identity. 18-month project. Hybrid environment needed during transition. Competitor Azure AD is free with M365.",
    "Digital transformation project kicked off. Customer moving to cloud-first architecture. Legacy apps need to be secured. Access Gateway discussion needed.",
    "Customer evaluating Zero Trust architecture. Identity is central to their strategy. Looking to expand from SSO to full workforce identity suite.",
    "Customer acquired company using competitor product. Integration decision pending - opportunity to expand or risk of churn. Need competitive displacement playbook.",
    "New CTO starting with cloud-first mandate. Previously worked at company using our competitor. Need to win them over quickly with technical deep dive."
]

security_insights = [
    "Ransomware attack at peer company elevated security to board level. Customer fast-tracking privileged access management. Budget cycle being bypassed.",
    "Major compliance audit in 60 days. Customer stressed about access governance gaps. Needs to demonstrate least privilege enforcement.",
    "Customer's MFA coverage at 60%, need to reach 100% in 90 days after board mandate. Strong opportunity for Adaptive MFA expansion.",
    "Recent security incident at customer site. Identity was the attack vector. Customer now prioritizing identity security investments.",
    "Customer mentioned insurance carrier requiring stronger identity controls. Access certification automation is now a must-have."
]

# AE and SE names
ae_names = ["Sarah Johnson", "Michael Chen", "Emily Rodriguez", "James Williams", "Amanda Thompson", 
            "Robert Kim", "Jennifer Martinez", "David Park", "Lisa Anderson", "Chris Taylor"]
se_names = ["Kevin Martinez", "Rachel Green", "Daniel Harris", "Jessica White", "Ryan Johnson",
            "Maria Rodriguez", "Brandon Lee", "Ashley Taylor", "Tyler Smith", "Nicole Garcia"]

call_types = ["Quarterly Business Review", "Renewal Discussion", "Expansion Opportunity", 
              "Technical Deep Dive", "Executive Briefing", "Product Demo", "Implementation Review"]

sentiments = ["Very Positive", "Positive", "Neutral", "Concerned", "Cautious"]

def generate_transcript(account_name, industry, healthscore, city, state, employees, carr):
    """Generate a realistic call transcript with business insights."""
    
    ae = random.choice(ae_names)
    se = random.choice(se_names)
    call_type = random.choice(call_types)
    call_date = datetime.now() - timedelta(days=random.randint(1, 180))
    duration = random.randint(30, 75)
    sentiment = random.choice(sentiments)
    
    # Select insights based on random factors
    insight_category = random.choice(["fiscal", "layoff", "tech", "security"])
    if insight_category == "fiscal":
        key_insight = random.choice(fiscal_insights)
    elif insight_category == "layoff":
        key_insight = random.choice(layoff_insights)
    elif insight_category == "tech":
        key_insight = random.choice(tech_insights)
    else:
        key_insight = random.choice(security_insights)
    
    user_count = int(employees * random.uniform(0.3, 0.8))
    
    transcript = f"""GONG CALL TRANSCRIPT
{'='*60}

CALL DETAILS
------------
Account: {account_name}
Industry: {industry}
Location: {city}, {state}
Health Score: {healthscore}
Current CARR: ${carr:,.2f}

Date: {call_date.strftime('%Y-%m-%d')}
Duration: {duration} minutes
Type: {call_type}
Sentiment: {sentiment}

PARTICIPANTS
------------
Account Executive: {ae}
Sales Engineer: {se}
Customer: VP of IT, Director of Security

KEY INSIGHTS
------------
{key_insight}

TRANSCRIPT
----------

[00:00] {ae}: Thanks for joining today's call. How are things going at {account_name}?

[00:30] Customer: Good to connect. Things have been quite busy lately with {random.choice(['budget planning', 'organizational changes', 'our digital transformation', 'security initiatives', 'compliance preparation'])}.

[02:00] {ae}: I understand. Before we dive in, I wanted to check - how has the identity platform been working for your team?

[03:00] Customer: Overall it's been solid. We have about {user_count:,} users active now. {random.choice(['SSO adoption is at 95% which is great.', 'MFA rollout completed last quarter.', 'The team really likes the self-service capabilities.', 'Were still working on getting all applications integrated.'])}

[05:00] {se}: That's great to hear. Are there any technical challenges or areas where you'd like more support?

[06:30] Customer: Actually yes - {random.choice(['we need better reporting on access patterns', 'the API rate limits are causing some issues', 'we want to automate more of our provisioning workflows', 'we need to improve our offboarding process'])}. 

[10:00] {ae}: Let me make a note of that. Now, I wanted to discuss {random.choice(['your upcoming renewal', 'some new capabilities that might help', 'how we can support your security goals', 'the roadmap'])}...

[12:00] Customer: Before we get into that, I should mention - {key_insight.split('.')[0]}.

[15:00] {ae}: That's really helpful context. How do you see this impacting your identity strategy?

[18:00] Customer: {random.choice([
    'We need to be more efficient with our current tools. Automation is key.',
    'Security is still a top priority, so were protected from cuts. But we need to show ROI.',
    'This actually creates an opportunity. We have budget allocated that needs to be used.',
    'Were evaluating all our vendors. Need to make sure were getting value.',
    'The new leadership wants to see a comprehensive identity strategy.'
])}

[25:00] {se}: I'd like to show you how {random.choice([
    'Lifecycle Management can automate your onboarding and offboarding',
    'Access Governance can help with your compliance requirements',
    'our Workflows product can handle complex provisioning scenarios',
    'Privileged Access Management protects your critical infrastructure',
    'our API Access Management secures your developer APIs'
])}. Let me share my screen...

[35:00] Customer: This is interesting. {random.choice([
    'Can you send me more information on pricing?',
    'We should schedule a deeper technical session.',
    'Let me discuss with my team and get back to you.',
    'This could address several of our challenges.',
    'How does this compare to what competitors offer?'
])}

[45:00] {ae}: Absolutely. To summarize our discussion today:
- Current usage is going well with {user_count:,} users
- Key focus area: {random.choice(['automation', 'compliance', 'security', 'efficiency', 'consolidation'])}
- Next steps: {random.choice([
    'Send detailed proposal by end of week',
    'Schedule technical deep dive with your team',
    'Connect you with a reference customer in your industry',
    'Prepare ROI analysis for leadership presentation',
    'Set up POC environment for evaluation'
])}

[48:00] Customer: Sounds good. Thanks for your time today.

[48:30] {ae}: Thank you! We'll follow up shortly.

[END OF TRANSCRIPT]

FOLLOW-UP ACTIONS
-----------------
1. Send {random.choice(['proposal', 'pricing', 'ROI calculator', 'technical documentation', 'customer references'])} by {(call_date + timedelta(days=3)).strftime('%Y-%m-%d')}
2. Schedule {random.choice(['technical deep dive', 'executive briefing', 'POC kickoff', 'security review', 'roadmap discussion'])}
3. Update opportunity in Salesforce
4. Notify {random.choice(['CSM', 'leadership', 'product team', 'support', 'professional services'])} about customer feedback

COMPETITIVE INTELLIGENCE
------------------------
{random.choice([
    'Customer mentioned evaluating Microsoft Entra ID - free with M365 license is key concern.',
    'Competitor Ping Identity came up in discussion - customer likes their pricing model.',
    'No competitive pressure currently - customer happy with our platform.',
    'Customer has legacy ForgeRock implementation - opportunity for displacement.',
    'CyberArk mentioned for PAM - we should highlight our integrated approach.'
])}
"""
    return transcript, call_date

# Generate transcripts
transcript_count = 0
for account in accounts:
    account_id, account_name, industry, healthscore, city, state, employees, carr = account
    
    # Generate 1-3 transcripts per account
    num_transcripts = random.randint(1, 3)
    
    for i in range(num_transcripts):
        transcript, call_date = generate_transcript(
            account_name, industry, healthscore, city, state, employees, carr
        )
        
        # Create filename
        safe_name = account_name.replace(" ", "_").replace("&", "and").replace("/", "-")
        date_str = call_date.strftime("%Y-%m-%d")
        filename = f"{safe_name}_{date_str}_call_{i+1}.txt"
        filepath = os.path.join(output_dir, filename)
        
        # Write transcript
        with open(filepath, 'w') as f:
            f.write(transcript)
        
        transcript_count += 1
        if transcript_count % 20 == 0:
            print(f"Generated {transcript_count} transcripts...")

print(f"\nComplete! Generated {transcript_count} transcript files in {output_dir}")
cursor.close()
conn.close()
