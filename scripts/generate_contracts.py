"""
Generate PDF Contracts
======================
Creates PDF contract files for all customers and uploads to Snowflake stage.

Usage:
    python generate_contracts.py

Prerequisites:
    pip install snowflake-connector-python reportlab
"""

import os
import snowflake.connector
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib.enums import TA_CENTER, TA_LEFT

# Connect to Snowflake
conn = snowflake.connector.connect(
    connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "sfsenorthamerica-demo351_aws"
)

# Create output directory
output_dir = os.path.join(os.path.dirname(__file__), "..", "contracts_pdf")
os.makedirs(output_dir, exist_ok=True)

# Fetch contract data with subscriptions
cursor = conn.cursor()
cursor.execute("""
    SELECT 
        c.CONTRACT_ID,
        c.CONTRACT_NUMBER,
        c.ACCOUNT_ID,
        a.ACCOUNT_NAME,
        a.BILLING_STREET,
        a.BILLING_CITY,
        a.BILLING_STATE,
        a.BILLING_POSTALCODE,
        a.BILLING_COUNTRY,
        a.INDUSTRY,
        c.START_DATE,
        c.END_DATE,
        c.CONTRACT_TERM,
        c.AUTO_RENEW,
        c.CUSTOMER_SIGNED_DATE,
        c.CUSTOMER_SIGNED_TITLE,
        c.TCV,
        c.MRR,
        c.ARR
    FROM PROD.RAW.SFDC_CONTRACT c
    JOIN PROD.RAW.SFDC_ACCOUNT a ON c.ACCOUNT_ID = a.ACCOUNT_ID
    ORDER BY c.CONTRACT_NUMBER
""")
contracts = cursor.fetchall()

print(f"Generating {len(contracts)} PDF contracts...")

# Styles
styles = getSampleStyleSheet()
title_style = ParagraphStyle(
    'CustomTitle',
    parent=styles['Heading1'],
    fontSize=18,
    spaceAfter=30,
    alignment=TA_CENTER
)
heading_style = ParagraphStyle(
    'CustomHeading',
    parent=styles['Heading2'],
    fontSize=14,
    spaceAfter=12,
    spaceBefore=20
)
normal_style = styles['Normal']

def generate_contract_pdf(contract_data, subscriptions):
    """Generate a PDF contract."""
    (contract_id, contract_number, account_id, account_name, street, city, state, 
     postal, country, industry, start_date, end_date, term, auto_renew,
     signed_date, signed_title, tcv, mrr, arr) = contract_data
    
    filename = f"contract_{contract_number}.pdf"
    filepath = os.path.join(output_dir, filename)
    
    doc = SimpleDocTemplate(filepath, pagesize=letter,
                           rightMargin=72, leftMargin=72,
                           topMargin=72, bottomMargin=72)
    
    story = []
    
    # Title
    story.append(Paragraph("SOFTWARE LICENSE AND SERVICES AGREEMENT", title_style))
    story.append(Spacer(1, 12))
    
    # Contract info
    story.append(Paragraph(f"<b>Contract Number:</b> {contract_number}", normal_style))
    story.append(Paragraph(f"<b>Effective Date:</b> {start_date}", normal_style))
    story.append(Paragraph(f"<b>End Date:</b> {end_date}", normal_style))
    story.append(Spacer(1, 20))
    
    # Parties
    story.append(Paragraph("PARTIES", heading_style))
    story.append(Paragraph(
        f'This Agreement is entered into between <b>SecureID Solutions</b> ("Provider") '
        f'and <b>{account_name}</b> ("Customer").',
        normal_style
    ))
    story.append(Spacer(1, 12))
    
    # Customer info
    story.append(Paragraph("CUSTOMER INFORMATION", heading_style))
    story.append(Paragraph(f"<b>Company:</b> {account_name}", normal_style))
    story.append(Paragraph(f"<b>Address:</b> {street}", normal_style))
    story.append(Paragraph(f"<b>City, State, ZIP:</b> {city}, {state} {postal}", normal_style))
    story.append(Paragraph(f"<b>Country:</b> {country}", normal_style))
    story.append(Paragraph(f"<b>Industry:</b> {industry}", normal_style))
    story.append(Spacer(1, 12))
    
    # Contract details
    story.append(Paragraph("CONTRACT DETAILS", heading_style))
    story.append(Paragraph(f"<b>Term:</b> {term} months", normal_style))
    story.append(Paragraph(f"<b>Auto-Renewal:</b> {'Yes' if auto_renew else 'No'}", normal_style))
    story.append(Paragraph(f"<b>Currency:</b> USD", normal_style))
    story.append(Spacer(1, 12))
    
    # Products table
    story.append(Paragraph("LICENSED PRODUCTS AND SERVICES", heading_style))
    
    if subscriptions:
        table_data = [['Product', 'Code', 'Users', 'List Price', 'Discount', 'Price', 'Monthly']]
        for sub in subscriptions:
            product_name, product_code, quantity, list_price, discount, customer_price, mrr_sub = sub
            table_data.append([
                product_name[:30] + '...' if len(product_name) > 30 else product_name,
                product_code,
                f"{int(quantity):,}",
                f"${list_price:.2f}",
                f"{discount:.0f}%",
                f"${customer_price:.2f}",
                f"${mrr_sub:,.2f}"
            ])
        
        table = Table(table_data, colWidths=[1.8*inch, 0.5*inch, 0.6*inch, 0.7*inch, 0.6*inch, 0.6*inch, 0.9*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 9),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ]))
        story.append(table)
    
    story.append(Spacer(1, 20))
    
    # Pricing summary
    story.append(Paragraph("PRICING SUMMARY", heading_style))
    pricing_data = [
        ['Description', 'Amount'],
        ['Monthly Recurring Revenue (MRR)', f"${mrr:,.2f}"],
        ['Annual Recurring Revenue (ARR)', f"${arr:,.2f}"],
        ['Total Contract Value (TCV)', f"${tcv:,.2f}"]
    ]
    pricing_table = Table(pricing_data, colWidths=[3*inch, 2*inch])
    pricing_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
    ]))
    story.append(pricing_table)
    story.append(Spacer(1, 20))
    
    # Terms
    story.append(Paragraph("TERMS AND CONDITIONS", heading_style))
    terms = [
        "1. <b>Grant of License:</b> Provider grants Customer a non-exclusive, non-transferable license to use the products listed above for the term of this Agreement.",
        "2. <b>Payment Terms:</b> Net 30 days from invoice date. Late payments accrue interest at 1.5% per month.",
        "3. <b>Data Protection:</b> Provider will process Customer data in accordance with applicable data protection laws and the Data Processing Agreement.",
        "4. <b>Service Level:</b> Provider commits to 99.9% uptime availability. Credits provided for downtime exceeding SLA.",
        "5. <b>Support:</b> 24/7 technical support included via phone, email, and chat.",
        "6. <b>Confidentiality:</b> Both parties agree to maintain confidentiality of proprietary information."
    ]
    for term in terms:
        story.append(Paragraph(term, normal_style))
        story.append(Spacer(1, 6))
    
    story.append(Spacer(1, 30))
    
    # Signatures
    story.append(Paragraph("SIGNATURES", heading_style))
    sig_data = [
        ['Customer', 'Provider'],
        [f"Title: {signed_title}", "Title: VP of Sales"],
        [f"Date: {signed_date}", f"Date: {signed_date}"],
        ['_' * 30, '_' * 30],
        [f"{account_name}", "SecureID Solutions"]
    ]
    sig_table = Table(sig_data, colWidths=[3*inch, 3*inch])
    sig_table.setStyle(TableStyle([
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
    ]))
    story.append(sig_table)
    
    # Build PDF
    doc.build(story)
    return filename

# Generate all contracts
for i, contract in enumerate(contracts):
    contract_id = contract[0]
    
    # Get subscriptions for this contract
    cursor.execute(f"""
        SELECT 
            p.PRODUCT_NAME,
            p.PRODUCT_CODE,
            s.QUANTITY,
            s.LIST_PRICE,
            s.DISCOUNT,
            s.CUSTOMER_PRICE,
            s.MRR
        FROM PROD.RAW.SFDC_SUBSCRIPTION_CPQ s
        JOIN PROD.RAW.SFDC_PRODUCT p ON s.PRODUCT_ID = p.PRODUCT_ID
        WHERE s.CONTRACT_ID = '{contract_id}'
        ORDER BY p.PRODUCT_NAME
    """)
    subscriptions = cursor.fetchall()
    
    filename = generate_contract_pdf(contract, subscriptions)
    
    if (i + 1) % 25 == 0:
        print(f"Generated {i + 1} contracts...")

print(f"\nGenerated {len(contracts)} PDF contracts in {output_dir}")

# Upload to Snowflake stage
print("\nUploading contracts to Snowflake stage...")
cursor.execute(f"PUT 'file://{output_dir}/*.pdf' @PROD.RAW.CONTRACTS_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
print("Upload complete!")

# Refresh directory
cursor.execute("ALTER STAGE PROD.RAW.CONTRACTS_STAGE REFRESH")

# Verify
cursor.execute("LIST @PROD.RAW.CONTRACTS_STAGE")
files = cursor.fetchall()
print(f"\nUploaded {len(files)} files to @PROD.RAW.CONTRACTS_STAGE")

cursor.close()
conn.close()
