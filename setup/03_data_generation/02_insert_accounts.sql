-- ============================================================
-- Step 3b: Generate 250 Fortune 500 Accounts
-- ============================================================
-- Uses real Fortune 500 company names spread across US timezones

-- First, truncate if re-running
TRUNCATE TABLE IF EXISTS PROD.RAW.SFDC_ACCOUNT;

INSERT INTO PROD.RAW.SFDC_ACCOUNT (
    ACCOUNT_ID, PARENT_ACCOUNT_ID, ACCOUNT_NAME, ACCOUNT_STATUS, ACCOUNT_TYPE,
    CREATED_DATE, CUSTOMER_ACQUISITION_DATE, RENEWAL_DATE, CARR, CARR_USD,
    BILLING_STREET, BILLING_CITY, BILLING_STATE, BILLING_POSTALCODE, BILLING_COUNTRY,
    GEOGRAPHY, TERRITORY, TIMEZONE, INDUSTRY, SUB_INDUSTRY, PRIMARY_INDUSTRY,
    ANNUAL_REVENUE, NUMBER_OF_EMPLOYEES, WEBSITE, HEALTHSCORE,
    TOP_ACCOUNT, NAMED_ACCOUNT, IS_CUSTOMER, PAYING_CUSTOMER
)
WITH fortune500_companies AS (
    SELECT column1 AS company_name, column2 AS industry, column3 AS sub_industry, 
           column4 AS hq_city, column5 AS hq_state, column6 AS website,
           column7 AS revenue_billions, column8 AS employees
    FROM VALUES
        -- Technology
        ('Amazon', 'Technology', 'E-commerce & Cloud', 'Seattle', 'WA', 'amazon.com', 575, 1540000),
        ('Apple', 'Technology', 'Consumer Electronics', 'Cupertino', 'CA', 'apple.com', 394, 164000),
        ('Alphabet', 'Technology', 'Internet Services', 'Mountain View', 'CA', 'google.com', 307, 190000),
        ('Microsoft', 'Technology', 'Software', 'Redmond', 'WA', 'microsoft.com', 245, 228000),
        ('Meta Platforms', 'Technology', 'Social Media', 'Menlo Park', 'CA', 'meta.com', 135, 86000),
        ('Dell Technologies', 'Technology', 'Hardware', 'Round Rock', 'TX', 'dell.com', 102, 133000),
        ('Intel', 'Technology', 'Semiconductors', 'Santa Clara', 'CA', 'intel.com', 63, 131900),
        ('IBM', 'Technology', 'IT Services', 'Armonk', 'NY', 'ibm.com', 61, 288300),
        ('Oracle', 'Technology', 'Enterprise Software', 'Austin', 'TX', 'oracle.com', 53, 164000),
        ('Cisco Systems', 'Technology', 'Networking', 'San Jose', 'CA', 'cisco.com', 57, 90400),
        ('Salesforce', 'Technology', 'CRM Software', 'San Francisco', 'CA', 'salesforce.com', 35, 79000),
        ('Adobe', 'Technology', 'Software', 'San Jose', 'CA', 'adobe.com', 20, 30000),
        ('Nvidia', 'Technology', 'Semiconductors', 'Santa Clara', 'CA', 'nvidia.com', 61, 29600),
        ('ServiceNow', 'Technology', 'Enterprise Software', 'Santa Clara', 'CA', 'servicenow.com', 9, 23000),
        ('Workday', 'Technology', 'HR Software', 'Pleasanton', 'CA', 'workday.com', 8, 18800),
        ('Palo Alto Networks', 'Technology', 'Cybersecurity', 'Santa Clara', 'CA', 'paloaltonetworks.com', 8, 15000),
        ('CrowdStrike', 'Technology', 'Cybersecurity', 'Austin', 'TX', 'crowdstrike.com', 3, 8500),
        ('Snowflake', 'Technology', 'Data Cloud', 'Bozeman', 'MT', 'snowflake.com', 3, 7000),
        ('Datadog', 'Technology', 'Monitoring', 'New York', 'NY', 'datadog.com', 2, 5500),
        ('Cloudflare', 'Technology', 'CDN & Security', 'San Francisco', 'CA', 'cloudflare.com', 1, 4000),
        
        -- Retail
        ('Walmart', 'Retail', 'General Merchandise', 'Bentonville', 'AR', 'walmart.com', 648, 2100000),
        ('Costco', 'Retail', 'Warehouse Clubs', 'Issaquah', 'WA', 'costco.com', 254, 316000),
        ('Home Depot', 'Retail', 'Home Improvement', 'Atlanta', 'GA', 'homedepot.com', 157, 475000),
        ('Target', 'Retail', 'General Merchandise', 'Minneapolis', 'MN', 'target.com', 107, 440000),
        ('Lowes', 'Retail', 'Home Improvement', 'Mooresville', 'NC', 'lowes.com', 86, 300000),
        ('Best Buy', 'Retail', 'Electronics', 'Richfield', 'MN', 'bestbuy.com', 43, 90000),
        ('Dollar General', 'Retail', 'Discount Stores', 'Goodlettsville', 'TN', 'dollargeneral.com', 39, 195000),
        ('Macys', 'Retail', 'Department Stores', 'New York', 'NY', 'macys.com', 24, 90000),
        ('Nordstrom', 'Retail', 'Department Stores', 'Seattle', 'WA', 'nordstrom.com', 15, 60000),
        ('Gap', 'Retail', 'Apparel', 'San Francisco', 'CA', 'gap.com', 15, 95000),
        
        -- Healthcare
        ('UnitedHealth Group', 'Healthcare', 'Health Insurance', 'Minnetonka', 'MN', 'unitedhealthgroup.com', 372, 440000),
        ('CVS Health', 'Healthcare', 'Pharmacy & Insurance', 'Woonsocket', 'RI', 'cvshealth.com', 357, 300000),
        ('Elevance Health', 'Healthcare', 'Health Insurance', 'Indianapolis', 'IN', 'elevancehealth.com', 175, 100000),
        ('Cigna', 'Healthcare', 'Health Insurance', 'Bloomfield', 'CT', 'cigna.com', 195, 74000),
        ('Humana', 'Healthcare', 'Health Insurance', 'Louisville', 'KY', 'humana.com', 115, 67000),
        ('HCA Healthcare', 'Healthcare', 'Hospital Systems', 'Nashville', 'TN', 'hcahealthcare.com', 64, 293000),
        ('Centene', 'Healthcare', 'Managed Care', 'St. Louis', 'MO', 'centene.com', 154, 74300),
        ('Cardinal Health', 'Healthcare', 'Pharmaceutical Distribution', 'Dublin', 'OH', 'cardinalhealth.com', 205, 48000),
        ('McKesson', 'Healthcare', 'Pharmaceutical Distribution', 'Irving', 'TX', 'mckesson.com', 309, 51000),
        ('AmerisourceBergen', 'Healthcare', 'Pharmaceutical Distribution', 'Conshohocken', 'PA', 'amerisourcebergen.com', 262, 44000),
        ('Pfizer', 'Healthcare', 'Pharmaceuticals', 'New York', 'NY', 'pfizer.com', 58, 83000),
        ('Johnson & Johnson', 'Healthcare', 'Pharmaceuticals & Devices', 'New Brunswick', 'NJ', 'jnj.com', 85, 152700),
        ('AbbVie', 'Healthcare', 'Pharmaceuticals', 'North Chicago', 'IL', 'abbvie.com', 58, 50000),
        ('Merck', 'Healthcare', 'Pharmaceuticals', 'Rahway', 'NJ', 'merck.com', 60, 70000),
        ('Bristol-Myers Squibb', 'Healthcare', 'Pharmaceuticals', 'New York', 'NY', 'bms.com', 45, 34000),
        
        -- Financial Services
        ('Berkshire Hathaway', 'Financial Services', 'Conglomerate', 'Omaha', 'NE', 'berkshirehathaway.com', 365, 396500),
        ('JPMorgan Chase', 'Financial Services', 'Banking', 'New York', 'NY', 'jpmorganchase.com', 178, 310000),
        ('Bank of America', 'Financial Services', 'Banking', 'Charlotte', 'NC', 'bankofamerica.com', 133, 217000),
        ('Wells Fargo', 'Financial Services', 'Banking', 'San Francisco', 'CA', 'wellsfargo.com', 82, 234000),
        ('Citigroup', 'Financial Services', 'Banking', 'New York', 'NY', 'citigroup.com', 102, 240000),
        ('Goldman Sachs', 'Financial Services', 'Investment Banking', 'New York', 'NY', 'goldmansachs.com', 47, 49100),
        ('Morgan Stanley', 'Financial Services', 'Investment Banking', 'New York', 'NY', 'morganstanley.com', 61, 82000),
        ('American Express', 'Financial Services', 'Credit Cards', 'New York', 'NY', 'americanexpress.com', 60, 77300),
        ('Capital One', 'Financial Services', 'Banking', 'McLean', 'VA', 'capitalone.com', 39, 52500),
        ('US Bancorp', 'Financial Services', 'Banking', 'Minneapolis', 'MN', 'usbank.com', 29, 77000),
        ('Charles Schwab', 'Financial Services', 'Brokerage', 'Westlake', 'TX', 'schwab.com', 21, 36000),
        ('BlackRock', 'Financial Services', 'Asset Management', 'New York', 'NY', 'blackrock.com', 20, 21000),
        ('State Street', 'Financial Services', 'Asset Management', 'Boston', 'MA', 'statestreet.com', 12, 53000),
        ('Fidelity', 'Financial Services', 'Asset Management', 'Boston', 'MA', 'fidelity.com', 28, 74000),
        ('Progressive', 'Financial Services', 'Insurance', 'Mayfield Village', 'OH', 'progressive.com', 62, 60000),
        ('Allstate', 'Financial Services', 'Insurance', 'Northbrook', 'IL', 'allstate.com', 58, 54500),
        ('Travelers', 'Financial Services', 'Insurance', 'New York', 'NY', 'travelers.com', 41, 30800),
        ('MetLife', 'Financial Services', 'Insurance', 'New York', 'NY', 'metlife.com', 69, 43000),
        ('Prudential Financial', 'Financial Services', 'Insurance', 'Newark', 'NJ', 'prudential.com', 51, 40000),
        ('AIG', 'Financial Services', 'Insurance', 'New York', 'NY', 'aig.com', 47, 26200),
        
        -- Energy
        ('ExxonMobil', 'Energy', 'Oil & Gas', 'Irving', 'TX', 'exxonmobil.com', 413, 62000),
        ('Chevron', 'Energy', 'Oil & Gas', 'San Ramon', 'CA', 'chevron.com', 246, 43846),
        ('ConocoPhillips', 'Energy', 'Oil & Gas', 'Houston', 'TX', 'conocophillips.com', 79, 10500),
        ('Phillips 66', 'Energy', 'Oil & Gas', 'Houston', 'TX', 'phillips66.com', 176, 14000),
        ('Valero Energy', 'Energy', 'Oil & Gas', 'San Antonio', 'TX', 'valero.com', 177, 10015),
        ('Marathon Petroleum', 'Energy', 'Oil & Gas', 'Findlay', 'OH', 'marathonpetroleum.com', 180, 18200),
        ('Duke Energy', 'Energy', 'Utilities', 'Charlotte', 'NC', 'duke-energy.com', 29, 27600),
        ('Southern Company', 'Energy', 'Utilities', 'Atlanta', 'GA', 'southerncompany.com', 29, 27000),
        ('Dominion Energy', 'Energy', 'Utilities', 'Richmond', 'VA', 'dominionenergy.com', 17, 17200),
        ('NextEra Energy', 'Energy', 'Utilities', 'Juno Beach', 'FL', 'nexteraenergy.com', 28, 15000),
        
        -- Transportation & Logistics
        ('UPS', 'Transportation', 'Package Delivery', 'Atlanta', 'GA', 'ups.com', 100, 500000),
        ('FedEx', 'Transportation', 'Package Delivery', 'Memphis', 'TN', 'fedex.com', 90, 518000),
        ('Delta Air Lines', 'Transportation', 'Airlines', 'Atlanta', 'GA', 'delta.com', 58, 100000),
        ('United Airlines', 'Transportation', 'Airlines', 'Chicago', 'IL', 'united.com', 53, 99000),
        ('American Airlines', 'Transportation', 'Airlines', 'Fort Worth', 'TX', 'aa.com', 53, 128900),
        ('Southwest Airlines', 'Transportation', 'Airlines', 'Dallas', 'TX', 'southwest.com', 26, 74000),
        ('Union Pacific', 'Transportation', 'Railroads', 'Omaha', 'NE', 'up.com', 25, 30000),
        ('Norfolk Southern', 'Transportation', 'Railroads', 'Atlanta', 'GA', 'nscorp.com', 12, 19300),
        ('CSX', 'Transportation', 'Railroads', 'Jacksonville', 'FL', 'csx.com', 15, 22000),
        ('JB Hunt', 'Transportation', 'Trucking', 'Lowell', 'AR', 'jbhunt.com', 15, 35000),
        
        -- Telecommunications
        ('AT&T', 'Telecommunications', 'Wireless', 'Dallas', 'TX', 'att.com', 121, 160700),
        ('Verizon', 'Telecommunications', 'Wireless', 'New York', 'NY', 'verizon.com', 137, 117100),
        ('T-Mobile', 'Telecommunications', 'Wireless', 'Bellevue', 'WA', 'tmobile.com', 80, 71000),
        ('Comcast', 'Telecommunications', 'Cable & Internet', 'Philadelphia', 'PA', 'comcast.com', 121, 186000),
        ('Charter Communications', 'Telecommunications', 'Cable & Internet', 'Stamford', 'CT', 'charter.com', 55, 101000),
        
        -- Automotive
        ('General Motors', 'Automotive', 'Auto Manufacturing', 'Detroit', 'MI', 'gm.com', 172, 167000),
        ('Ford Motor', 'Automotive', 'Auto Manufacturing', 'Dearborn', 'MI', 'ford.com', 176, 177000),
        ('Tesla', 'Automotive', 'Electric Vehicles', 'Austin', 'TX', 'tesla.com', 97, 140000),
        ('Stellantis', 'Automotive', 'Auto Manufacturing', 'Auburn Hills', 'MI', 'stellantis.com', 189, 281000),
        ('AutoNation', 'Automotive', 'Auto Dealerships', 'Fort Lauderdale', 'FL', 'autonation.com', 27, 21000),
        ('Penske Automotive', 'Automotive', 'Auto Dealerships', 'Bloomfield Hills', 'MI', 'penskeautomotive.com', 29, 29000),
        
        -- Consumer Goods
        ('Procter & Gamble', 'Consumer Goods', 'Household Products', 'Cincinnati', 'OH', 'pg.com', 84, 107000),
        ('PepsiCo', 'Consumer Goods', 'Beverages & Snacks', 'Purchase', 'NY', 'pepsico.com', 91, 318000),
        ('Coca-Cola', 'Consumer Goods', 'Beverages', 'Atlanta', 'GA', 'coca-colacompany.com', 46, 82500),
        ('Nike', 'Consumer Goods', 'Apparel & Footwear', 'Beaverton', 'OR', 'nike.com', 51, 83700),
        ('Colgate-Palmolive', 'Consumer Goods', 'Household Products', 'New York', 'NY', 'colgatepalmolive.com', 19, 33800),
        ('Kimberly-Clark', 'Consumer Goods', 'Household Products', 'Irving', 'TX', 'kimberly-clark.com', 20, 43000),
        ('Estee Lauder', 'Consumer Goods', 'Cosmetics', 'New York', 'NY', 'elcompanies.com', 16, 62000),
        ('Mondelez', 'Consumer Goods', 'Snacks', 'Chicago', 'IL', 'mondelezinternational.com', 36, 91000),
        ('General Mills', 'Consumer Goods', 'Food Products', 'Minneapolis', 'MN', 'generalmills.com', 20, 35000),
        ('Kellogg', 'Consumer Goods', 'Food Products', 'Battle Creek', 'MI', 'kelloggcompany.com', 16, 30000),
        ('Kraft Heinz', 'Consumer Goods', 'Food Products', 'Chicago', 'IL', 'kraftheinzcompany.com', 27, 37000),
        ('Campbell Soup', 'Consumer Goods', 'Food Products', 'Camden', 'NJ', 'campbellsoupcompany.com', 10, 14000),
        ('Hershey', 'Consumer Goods', 'Confectionery', 'Hershey', 'PA', 'thehersheycompany.com', 11, 20000),
        ('Clorox', 'Consumer Goods', 'Household Products', 'Oakland', 'CA', 'thecloroxcompany.com', 7, 9000),
        
        -- Industrial & Manufacturing
        ('3M', 'Industrial', 'Diversified Manufacturing', 'St. Paul', 'MN', '3m.com', 33, 92000),
        ('Honeywell', 'Industrial', 'Aerospace & Diversified', 'Charlotte', 'NC', 'honeywell.com', 36, 110000),
        ('Caterpillar', 'Industrial', 'Heavy Equipment', 'Irving', 'TX', 'caterpillar.com', 67, 109100),
        ('Deere & Company', 'Industrial', 'Farm Equipment', 'Moline', 'IL', 'deere.com', 61, 82200),
        ('Boeing', 'Industrial', 'Aerospace', 'Arlington', 'VA', 'boeing.com', 78, 156000),
        ('Lockheed Martin', 'Industrial', 'Defense', 'Bethesda', 'MD', 'lockheedmartin.com', 71, 116000),
        ('Raytheon Technologies', 'Industrial', 'Aerospace & Defense', 'Arlington', 'VA', 'rtx.com', 69, 182000),
        ('Northrop Grumman', 'Industrial', 'Defense', 'Falls Church', 'VA', 'northropgrumman.com', 41, 100500),
        ('General Dynamics', 'Industrial', 'Defense', 'Reston', 'VA', 'gd.com', 43, 112000),
        ('General Electric', 'Industrial', 'Diversified', 'Boston', 'MA', 'ge.com', 77, 125000),
        ('Emerson Electric', 'Industrial', 'Electronics', 'St. Louis', 'MO', 'emerson.com', 19, 86700),
        ('Parker Hannifin', 'Industrial', 'Motion & Control', 'Cleveland', 'OH', 'parker.com', 19, 62290),
        ('Illinois Tool Works', 'Industrial', 'Diversified Manufacturing', 'Glenview', 'IL', 'itw.com', 16, 46000),
        ('Eaton', 'Industrial', 'Electrical', 'Dublin', 'OH', 'eaton.com', 23, 92000),
        ('Rockwell Automation', 'Industrial', 'Automation', 'Milwaukee', 'WI', 'rockwellautomation.com', 9, 29000),
        
        -- Media & Entertainment
        ('Walt Disney', 'Media', 'Entertainment', 'Burbank', 'CA', 'thewaltdisneycompany.com', 89, 220000),
        ('Netflix', 'Media', 'Streaming', 'Los Gatos', 'CA', 'netflix.com', 34, 13000),
        ('Warner Bros Discovery', 'Media', 'Entertainment', 'New York', 'NY', 'wbd.com', 41, 35000),
        ('Paramount Global', 'Media', 'Entertainment', 'New York', 'NY', 'paramount.com', 30, 22000),
        ('Fox Corporation', 'Media', 'Broadcasting', 'New York', 'NY', 'foxcorporation.com', 14, 10000),
        ('News Corp', 'Media', 'Publishing', 'New York', 'NY', 'newscorp.com', 10, 24000),
        ('Live Nation', 'Media', 'Live Entertainment', 'Beverly Hills', 'CA', 'livenationentertainment.com', 23, 68400),
        ('Electronic Arts', 'Media', 'Video Games', 'Redwood City', 'CA', 'ea.com', 8, 13400),
        ('Activision Blizzard', 'Media', 'Video Games', 'Santa Monica', 'CA', 'activisionblizzard.com', 9, 13000),
        ('Take-Two Interactive', 'Media', 'Video Games', 'New York', 'NY', 'take2games.com', 5, 11580),
        
        -- Hotels & Restaurants
        ('McDonalds', 'Hospitality', 'Restaurants', 'Chicago', 'IL', 'mcdonalds.com', 25, 150000),
        ('Starbucks', 'Hospitality', 'Coffee & Food', 'Seattle', 'WA', 'starbucks.com', 36, 400000),
        ('Marriott International', 'Hospitality', 'Hotels', 'Bethesda', 'MD', 'marriott.com', 24, 141000),
        ('Hilton', 'Hospitality', 'Hotels', 'McLean', 'VA', 'hilton.com', 11, 159000),
        ('Hyatt Hotels', 'Hospitality', 'Hotels', 'Chicago', 'IL', 'hyatt.com', 7, 53000),
        ('Yum Brands', 'Hospitality', 'Restaurants', 'Louisville', 'KY', 'yum.com', 7, 36000),
        ('Chipotle', 'Hospitality', 'Restaurants', 'Newport Beach', 'CA', 'chipotle.com', 11, 117000),
        ('Darden Restaurants', 'Hospitality', 'Restaurants', 'Orlando', 'FL', 'darden.com', 11, 175000),
        
        -- Real Estate
        ('CBRE Group', 'Real Estate', 'Commercial Real Estate', 'Dallas', 'TX', 'cbre.com', 32, 130000),
        ('Jones Lang LaSalle', 'Real Estate', 'Commercial Real Estate', 'Chicago', 'IL', 'jll.com', 22, 106000),
        ('Prologis', 'Real Estate', 'Industrial REIT', 'San Francisco', 'CA', 'prologis.com', 8, 2600),
        ('Simon Property Group', 'Real Estate', 'Retail REIT', 'Indianapolis', 'IN', 'simon.com', 6, 2700),
        ('Public Storage', 'Real Estate', 'Self Storage REIT', 'Glendale', 'CA', 'publicstorage.com', 5, 7000),
        
        -- Additional Companies to reach 250
        ('Sysco', 'Food Distribution', 'Foodservice Distribution', 'Houston', 'TX', 'sysco.com', 76, 72000),
        ('US Foods', 'Food Distribution', 'Foodservice Distribution', 'Rosemont', 'IL', 'usfoods.com', 36, 30000),
        ('Performance Food Group', 'Food Distribution', 'Foodservice Distribution', 'Richmond', 'VA', 'pfgc.com', 59, 38000),
        ('Arrow Electronics', 'Distribution', 'Electronics Distribution', 'Centennial', 'CO', 'arrow.com', 37, 22500),
        ('Avnet', 'Distribution', 'Electronics Distribution', 'Phoenix', 'AZ', 'avnet.com', 26, 15400),
        ('WW Grainger', 'Distribution', 'Industrial Distribution', 'Lake Forest', 'IL', 'grainger.com', 17, 26200),
        ('Fastenal', 'Distribution', 'Industrial Distribution', 'Winona', 'MN', 'fastenal.com', 7, 23400),
        ('CDW', 'Distribution', 'IT Distribution', 'Vernon Hills', 'IL', 'cdw.com', 24, 15200),
        ('Insight Enterprises', 'Distribution', 'IT Distribution', 'Tempe', 'AZ', 'insight.com', 10, 14400),
        ('World Fuel Services', 'Distribution', 'Fuel Distribution', 'Miami', 'FL', 'wfscorp.com', 59, 4200),
        ('Henry Schein', 'Distribution', 'Healthcare Distribution', 'Melville', 'NY', 'henryschein.com', 13, 24400),
        ('Accenture', 'Professional Services', 'Consulting', 'New York', 'NY', 'accenture.com', 64, 738000),
        ('Cognizant', 'Professional Services', 'IT Services', 'Teaneck', 'NJ', 'cognizant.com', 19, 351500),
        ('Automatic Data Processing', 'Professional Services', 'HR Services', 'Roseland', 'NJ', 'adp.com', 19, 63000),
        ('Paychex', 'Professional Services', 'HR Services', 'Rochester', 'NY', 'paychex.com', 5, 16600),
        ('ManpowerGroup', 'Professional Services', 'Staffing', 'Milwaukee', 'WI', 'manpowergroup.com', 18, 26000),
        ('Robert Half', 'Professional Services', 'Staffing', 'Menlo Park', 'CA', 'roberthalf.com', 6, 12400),
        ('Cintas', 'Professional Services', 'Uniforms & Facilities', 'Cincinnati', 'OH', 'cintas.com', 9, 44000),
        ('Rollins', 'Professional Services', 'Pest Control', 'Atlanta', 'GA', 'rollins.com', 3, 19200),
        ('Republic Services', 'Waste Management', 'Waste Collection', 'Phoenix', 'AZ', 'republicservices.com', 15, 41000),
        ('Waste Management', 'Waste Management', 'Waste Collection', 'Houston', 'TX', 'wm.com', 21, 48000),
        ('Quanta Services', 'Construction', 'Infrastructure', 'Houston', 'TX', 'quantaservices.com', 23, 54800),
        ('Fluor', 'Construction', 'Engineering & Construction', 'Irving', 'TX', 'fluor.com', 14, 32000),
        ('Jacobs Engineering', 'Construction', 'Engineering', 'Dallas', 'TX', 'jacobs.com', 16, 60000),
        ('AECOM', 'Construction', 'Infrastructure', 'Dallas', 'TX', 'aecom.com', 14, 51000),
        ('Leidos', 'Government Services', 'IT & Engineering', 'Reston', 'VA', 'leidos.com', 16, 47000),
        ('Booz Allen Hamilton', 'Government Services', 'Consulting', 'McLean', 'VA', 'boozallen.com', 10, 34400),
        ('Science Applications', 'Government Services', 'IT Services', 'Reston', 'VA', 'saic.com', 7, 24000),
        ('DXC Technology', 'Technology', 'IT Services', 'Ashburn', 'VA', 'dxc.com', 14, 130000),
        ('Hewlett Packard Enterprise', 'Technology', 'IT Infrastructure', 'Houston', 'TX', 'hpe.com', 29, 60000),
        ('HP Inc', 'Technology', 'PC & Printers', 'Palo Alto', 'CA', 'hp.com', 54, 58000),
        ('Western Digital', 'Technology', 'Storage', 'San Jose', 'CA', 'westerndigital.com', 12, 51000),
        ('Seagate Technology', 'Technology', 'Storage', 'Fremont', 'CA', 'seagate.com', 8, 29000),
        ('Micron Technology', 'Technology', 'Memory', 'Boise', 'ID', 'micron.com', 31, 48000),
        ('Applied Materials', 'Technology', 'Semiconductor Equipment', 'Santa Clara', 'CA', 'appliedmaterials.com', 27, 35500),
        ('Lam Research', 'Technology', 'Semiconductor Equipment', 'Fremont', 'CA', 'lamresearch.com', 17, 19200),
        ('KLA Corporation', 'Technology', 'Semiconductor Equipment', 'Milpitas', 'CA', 'kla.com', 11, 15500),
        ('Texas Instruments', 'Technology', 'Semiconductors', 'Dallas', 'TX', 'ti.com', 18, 34000),
        ('Analog Devices', 'Technology', 'Semiconductors', 'Wilmington', 'MA', 'analog.com', 12, 26000),
        ('Broadcom', 'Technology', 'Semiconductors', 'San Jose', 'CA', 'broadcom.com', 39, 20000),
        ('Qualcomm', 'Technology', 'Semiconductors', 'San Diego', 'CA', 'qualcomm.com', 44, 51000),
        ('Advanced Micro Devices', 'Technology', 'Semiconductors', 'Santa Clara', 'CA', 'amd.com', 24, 26000),
        ('Marvell Technology', 'Technology', 'Semiconductors', 'Wilmington', 'DE', 'marvell.com', 6, 8400),
        ('Intuit', 'Technology', 'Financial Software', 'Mountain View', 'CA', 'intuit.com', 16, 18200),
        ('Autodesk', 'Technology', 'Design Software', 'San Francisco', 'CA', 'autodesk.com', 6, 14100),
        ('Synopsys', 'Technology', 'EDA Software', 'Sunnyvale', 'CA', 'synopsys.com', 6, 19400),
        ('Cadence Design', 'Technology', 'EDA Software', 'San Jose', 'CA', 'cadence.com', 4, 11200),
        ('Fortinet', 'Technology', 'Cybersecurity', 'Sunnyvale', 'CA', 'fortinet.com', 5, 14200),
        ('Zscaler', 'Technology', 'Cybersecurity', 'San Jose', 'CA', 'zscaler.com', 2, 7800),
        ('Splunk', 'Technology', 'Data Analytics', 'San Francisco', 'CA', 'splunk.com', 4, 8500),
        ('MongoDB', 'Technology', 'Database', 'New York', 'NY', 'mongodb.com', 2, 5500),
        ('Twilio', 'Technology', 'Communications API', 'San Francisco', 'CA', 'twilio.com', 4, 8100),
        ('Okta', 'Technology', 'Identity', 'San Francisco', 'CA', 'okta.com', 2, 6500),
        ('DocuSign', 'Technology', 'Digital Signatures', 'San Francisco', 'CA', 'docusign.com', 3, 7500),
        ('Atlassian', 'Technology', 'Collaboration Software', 'San Francisco', 'CA', 'atlassian.com', 4, 11900),
        ('Zoom Video', 'Technology', 'Video Communications', 'San Jose', 'CA', 'zoom.us', 4, 8400),
        ('Uber Technologies', 'Technology', 'Ride Sharing', 'San Francisco', 'CA', 'uber.com', 38, 32800),
        ('Lyft', 'Technology', 'Ride Sharing', 'San Francisco', 'CA', 'lyft.com', 4, 4100),
        ('DoorDash', 'Technology', 'Food Delivery', 'San Francisco', 'CA', 'doordash.com', 9, 19300),
        ('Airbnb', 'Technology', 'Travel Platform', 'San Francisco', 'CA', 'airbnb.com', 10, 6900),
        ('Block', 'Technology', 'Financial Technology', 'San Francisco', 'CA', 'block.xyz', 22, 14000),
        ('PayPal', 'Technology', 'Payments', 'San Jose', 'CA', 'paypal.com', 30, 29900),
        ('Visa', 'Financial Services', 'Payments', 'San Francisco', 'CA', 'visa.com', 35, 30300),
        ('Mastercard', 'Financial Services', 'Payments', 'Purchase', 'NY', 'mastercard.com', 26, 33400),
        ('Fiserv', 'Financial Services', 'Financial Technology', 'Milwaukee', 'WI', 'fiserv.com', 19, 40000),
        ('FIS', 'Financial Services', 'Financial Technology', 'Jacksonville', 'FL', 'fisglobal.com', 15, 65000),
        ('Global Payments', 'Financial Services', 'Payments', 'Atlanta', 'GA', 'globalpayments.com', 9, 27000),
        ('Discover Financial', 'Financial Services', 'Credit Cards', 'Riverwoods', 'IL', 'discover.com', 16, 21400),
        ('Synchrony Financial', 'Financial Services', 'Consumer Finance', 'Stamford', 'CT', 'synchrony.com', 13, 20000),
        ('Ally Financial', 'Financial Services', 'Auto Finance', 'Detroit', 'MI', 'ally.com', 9, 11200),
        ('Fifth Third Bancorp', 'Financial Services', 'Banking', 'Cincinnati', 'OH', 'fifththird.com', 8, 20000),
        ('KeyCorp', 'Financial Services', 'Banking', 'Cleveland', 'OH', 'key.com', 7, 17000),
        ('Regions Financial', 'Financial Services', 'Banking', 'Birmingham', 'AL', 'regions.com', 8, 19700),
        ('M&T Bank', 'Financial Services', 'Banking', 'Buffalo', 'NY', 'mtb.com', 9, 22700),
        ('Citizens Financial', 'Financial Services', 'Banking', 'Providence', 'RI', 'citizensbank.com', 9, 18000),
        ('Huntington Bancshares', 'Financial Services', 'Banking', 'Columbus', 'OH', 'huntington.com', 8, 20000),
        ('PNC Financial', 'Financial Services', 'Banking', 'Pittsburgh', 'PA', 'pnc.com', 22, 60000),
        ('Truist Financial', 'Financial Services', 'Banking', 'Charlotte', 'NC', 'truist.com', 23, 52000),
        ('First Republic Bank', 'Financial Services', 'Banking', 'San Francisco', 'CA', 'firstrepublic.com', 6, 7500),
        ('SVB Financial', 'Financial Services', 'Banking', 'Santa Clara', 'CA', 'svb.com', 7, 8500),
        ('Signature Bank', 'Financial Services', 'Banking', 'New York', 'NY', 'signatureny.com', 3, 2000),
        ('Hartford Financial', 'Financial Services', 'Insurance', 'Hartford', 'CT', 'thehartford.com', 24, 18800),
        ('Lincoln National', 'Financial Services', 'Insurance', 'Radnor', 'PA', 'lfg.com', 17, 10800),
        ('Principal Financial', 'Financial Services', 'Insurance', 'Des Moines', 'IA', 'principal.com', 18, 19500),
        ('Aflac', 'Financial Services', 'Insurance', 'Columbus', 'GA', 'aflac.com', 20, 12500),
        ('Unum Group', 'Financial Services', 'Insurance', 'Chattanooga', 'TN', 'unum.com', 13, 10400),
        ('Voya Financial', 'Financial Services', 'Insurance', 'New York', 'NY', 'voya.com', 7, 6500),
        ('Ameriprise Financial', 'Financial Services', 'Wealth Management', 'Minneapolis', 'MN', 'ameriprise.com', 17, 14400),
        ('Raymond James', 'Financial Services', 'Wealth Management', 'St. Petersburg', 'FL', 'raymondjames.com', 13, 17500),
        ('LPL Financial', 'Financial Services', 'Wealth Management', 'San Diego', 'CA', 'lpl.com', 11, 8200),
        ('Stifel Financial', 'Financial Services', 'Investment Banking', 'St. Louis', 'MO', 'stifel.com', 5, 9200),
        ('Northern Trust', 'Financial Services', 'Asset Management', 'Chicago', 'IL', 'northerntrust.com', 7, 23000),
        ('BNY Mellon', 'Financial Services', 'Asset Management', 'New York', 'NY', 'bnymellon.com', 18, 52600),
        ('Invesco', 'Financial Services', 'Asset Management', 'Atlanta', 'GA', 'invesco.com', 6, 8400),
        ('Franklin Resources', 'Financial Services', 'Asset Management', 'San Mateo', 'CA', 'franklinresources.com', 8, 9500),
        ('T Rowe Price', 'Financial Services', 'Asset Management', 'Baltimore', 'MD', 'troweprice.com', 7, 8200),
        ('Nasdaq', 'Financial Services', 'Exchange', 'New York', 'NY', 'nasdaq.com', 7, 6100),
        ('CME Group', 'Financial Services', 'Exchange', 'Chicago', 'IL', 'cmegroup.com', 6, 4500),
        ('Intercontinental Exchange', 'Financial Services', 'Exchange', 'Atlanta', 'GA', 'theice.com', 9, 9700),
        ('MSCI', 'Financial Services', 'Index Provider', 'New York', 'NY', 'msci.com', 3, 5400),
        ('S&P Global', 'Financial Services', 'Data & Analytics', 'New York', 'NY', 'spglobal.com', 13, 40450),
        ('Moodys', 'Financial Services', 'Credit Ratings', 'New York', 'NY', 'moodys.com', 6, 15700),
        ('Equifax', 'Financial Services', 'Credit Bureau', 'Atlanta', 'GA', 'equifax.com', 6, 15000),
        ('TransUnion', 'Financial Services', 'Credit Bureau', 'Chicago', 'IL', 'transunion.com', 4, 13000),
        ('Experian', 'Financial Services', 'Credit Bureau', 'Costa Mesa', 'CA', 'experian.com', 7, 22500),
        ('Verisk Analytics', 'Financial Services', 'Data Analytics', 'Jersey City', 'NJ', 'verisk.com', 3, 9100),
        ('Fair Isaac', 'Financial Services', 'Analytics', 'Bozeman', 'MT', 'fico.com', 2, 3900),
        ('Markel', 'Financial Services', 'Insurance', 'Glen Allen', 'VA', 'markel.com', 16, 21000),
        ('WR Berkley', 'Financial Services', 'Insurance', 'Greenwich', 'CT', 'wrberkley.com', 12, 8500),
        ('Arch Capital', 'Financial Services', 'Insurance', 'Hamilton', 'NJ', 'archgroup.com', 14, 6200),
        ('Reinsurance Group', 'Financial Services', 'Reinsurance', 'Chesterfield', 'MO', 'rgare.com', 20, 3200),
        ('Everest Re', 'Financial Services', 'Reinsurance', 'Warren', 'NJ', 'everestre.com', 15, 4000),
        ('Brown & Brown', 'Financial Services', 'Insurance Brokerage', 'Daytona Beach', 'FL', 'bbinsurance.com', 5, 16000),
        ('Arthur J Gallagher', 'Financial Services', 'Insurance Brokerage', 'Rolling Meadows', 'IL', 'ajg.com', 10, 52200),
        ('Marsh McLennan', 'Financial Services', 'Insurance Brokerage', 'New York', 'NY', 'marshmclennan.com', 23, 86000),
        ('Aon', 'Financial Services', 'Insurance Brokerage', 'Dublin', 'OH', 'aon.com', 13, 50000),
        ('Willis Towers Watson', 'Financial Services', 'Insurance Brokerage', 'Arlington', 'VA', 'wtwco.com', 9, 47000)
),
numbered_companies AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (ORDER BY revenue_billions DESC) AS rn
    FROM fortune500_companies
    QUALIFY rn <= 250
),
timezone_mapping AS (
    SELECT 
        company_name,
        industry,
        sub_industry,
        hq_city,
        hq_state,
        website,
        revenue_billions,
        employees,
        CASE 
            WHEN hq_state IN ('WA', 'OR', 'CA', 'NV') THEN 'Pacific'
            WHEN hq_state IN ('MT', 'ID', 'WY', 'UT', 'CO', 'AZ', 'NM') THEN 'Mountain'
            WHEN hq_state IN ('ND', 'SD', 'NE', 'KS', 'MN', 'IA', 'MO', 'WI', 'IL', 'IN', 'MI', 'OH', 'TX', 'OK', 'AR', 'LA', 'MS', 'AL', 'TN', 'KY') THEN 'Central'
            ELSE 'Eastern'
        END AS timezone,
        CASE 
            WHEN hq_state IN ('WA', 'OR', 'CA', 'NV') THEN 'West'
            WHEN hq_state IN ('MT', 'ID', 'WY', 'UT', 'CO', 'AZ', 'NM') THEN 'Mountain'
            WHEN hq_state IN ('ND', 'SD', 'NE', 'KS', 'MN', 'IA', 'MO', 'WI', 'IL', 'IN', 'MI', 'OH', 'TX', 'OK', 'AR', 'LA', 'MS', 'AL', 'TN', 'KY') THEN 'Central'
            ELSE 'East'
        END AS territory,
        ROW_NUMBER() OVER (ORDER BY revenue_billions DESC) AS rn
    FROM fortune500_companies
)
SELECT 
    'ACC' || LPAD(rn::VARCHAR, 6, '0') AS account_id,
    NULL AS parent_account_id,
    company_name AS account_name,
    CASE WHEN MOD(rn, 10) = 0 THEN 'Churned' ELSE 'Active' END AS account_status,
    'Customer' AS account_type,
    DATEADD(day, -MOD(ABS(HASH(company_name)), 1460) - 365, CURRENT_TIMESTAMP()) AS created_date,
    DATEADD(day, -MOD(ABS(HASH(company_name || 'acq')), 1095) - 180, CURRENT_DATE()) AS customer_acquisition_date,
    DATEADD(day, MOD(ABS(HASH(company_name || 'ren')), 335) + 30, CURRENT_DATE()) AS renewal_date,
    ROUND(50000 + MOD(ABS(HASH(company_name || 'carr')), 450000), 2) AS carr,
    ROUND(50000 + MOD(ABS(HASH(company_name || 'carr')), 450000), 2) AS carr_usd,
    (100 + MOD(ABS(HASH(company_name)), 9900))::VARCHAR || ' Corporate Drive' AS billing_street,
    hq_city AS billing_city,
    hq_state AS billing_state,
    CASE hq_state
        WHEN 'CA' THEN '9' || LPAD(MOD(ABS(HASH(company_name)), 10000)::VARCHAR, 4, '0')
        WHEN 'NY' THEN '1' || LPAD(MOD(ABS(HASH(company_name)), 10000)::VARCHAR, 4, '0')
        WHEN 'TX' THEN '7' || LPAD(MOD(ABS(HASH(company_name)), 10000)::VARCHAR, 4, '0')
        ELSE LPAD(MOD(ABS(HASH(company_name)), 100000)::VARCHAR, 5, '0')
    END AS billing_postalcode,
    'United States' AS billing_country,
    'Americas' AS geography,
    territory,
    timezone,
    industry,
    sub_industry,
    industry AS primary_industry,
    revenue_billions * 1000000000 AS annual_revenue,
    employees AS number_of_employees,
    website,
    CASE MOD(ABS(HASH(company_name || 'health')), 5)
        WHEN 0 THEN 'Excellent'
        WHEN 1 THEN 'Healthy'
        WHEN 2 THEN 'Good'
        WHEN 3 THEN 'At Risk'
        ELSE 'Critical'
    END AS healthscore,
    CASE WHEN revenue_billions > 100 THEN TRUE ELSE FALSE END AS top_account,
    CASE WHEN revenue_billions > 50 THEN TRUE ELSE FALSE END AS named_account,
    TRUE AS is_customer,
    TRUE AS paying_customer
FROM timezone_mapping
WHERE rn <= 250;

-- Verify account count and distribution
SELECT timezone, COUNT(*) as count FROM PROD.RAW.SFDC_ACCOUNT GROUP BY timezone ORDER BY timezone;
SELECT COUNT(*) as total_accounts FROM PROD.RAW.SFDC_ACCOUNT;
