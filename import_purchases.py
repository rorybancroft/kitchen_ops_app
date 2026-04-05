import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime

db_path = "/Users/rorybancroft/.openclaw/workspace/kitchen_ops_app/kitchen_ops_uga.db"
excel_path = "/Users/rorybancroft/.openclaw/workspace/FC Tracking UGA March 2026.xlsx"

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Clear existing March 2026 data to avoid duplicates just in case
cursor.execute("DELETE FROM purchases WHERE strftime('%Y-%m', purchase_date) = '2026-03'")

# Load all sheets
sheets = pd.read_excel(excel_path, sheet_name=None)

inserted_count = 0
total_spent = 0.0

for i in range(1, 32):
    sheet_name = f"Day {i}"
    if sheet_name in sheets:
        df = sheets[sheet_name]
        
        # Look for the header row where column 0 is 'Company'
        header_idx = None
        for idx, row in df.iterrows():
            if str(row.iloc[0]).strip().lower() == 'company':
                header_idx = idx
                break
                
        if header_idx is not None:
            # Re-read or just slice the dataframe
            df = df.iloc[header_idx + 1:].copy()
            # The columns are typically: 0: Company, 1: Invoice Number, 2: Invoice Total, 3: PO Number
            
            for _, row in df.iterrows():
                company = str(row.iloc[0]).strip()
                if company == 'nan' or company == '' or 'None' in company:
                    continue
                    
                invoice_number = str(row.iloc[1]).strip()
                if invoice_number == 'nan':
                    invoice_number = ""
                    
                try:
                    invoice_total = float(str(row.iloc[2]).replace('$', '').replace(',', '').strip())
                except ValueError:
                    invoice_total = 0.0
                    
                if invoice_total == 0.0 and company == "":
                    continue
                    
                po_number = ""
                if len(row) > 3:
                    po_number = str(row.iloc[3]).strip()
                    if po_number == 'nan':
                        po_number = ""
                        
                date_str = f"2026-03-{i:02d}"
                
                cursor.execute(
                    """
                    INSERT INTO purchases (purchase_date, company, invoice_number, invoice_total, po_number)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (date_str, company, invoice_number, invoice_total, po_number)
                )
                inserted_count += 1
                total_spent += invoice_total

conn.commit()
conn.close()

print(f"Successfully inserted {inserted_count} purchases for March 2026.")
print(f"Total spent: ${total_spent:.2f}")
