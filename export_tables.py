import sqlite3
import pandas as pd
import os

BASE_DIR = r"C:\Users\welcome\Desktop\Sales_Analytics_Project"
DB_PATH = os.path.join(BASE_DIR, "outputs", "sales_analytics.db")
EXPORT_DIR = os.path.join(BASE_DIR, "exports")

# Ensure exports folder exists
if os.path.exists(EXPORT_DIR):
    if not os.path.isdir(EXPORT_DIR):
        os.remove(EXPORT_DIR)
        os.makedirs(EXPORT_DIR)
else:
    os.makedirs(EXPORT_DIR)

tables = [
    "sa_fact_sales",
    "sa_dim_customer",
    "sa_dim_product",
    "sa_dim_date",
    "sa_customer_metrics",
    "sa_monthly_sales"
]

conn = sqlite3.connect(DB_PATH)

for table in tables:
    print("Exporting:", table)
    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    output_path = os.path.join(EXPORT_DIR, f"{table}.csv")
    df.to_csv(output_path, index=False)
    print("Saved:", output_path)

conn.close()
print("All tables exported successfully.")
