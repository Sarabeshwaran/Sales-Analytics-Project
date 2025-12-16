# sales_pipeline.py
# Updated pipeline that supports Excel (.xls) and normalizes column names

import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine
from pathlib import Path
import sys

# ==============
# CONFIG
# ==============
DATA_PATH = Path("US Superstore data.xls")   # update if different
OUTPUT_DIR = Path("outputs")
DB_PATH = OUTPUT_DIR / "sales_analytics.db"
TABLE_PREFIX = "sa_"

# ensure outputs folder exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

if not DATA_PATH.exists():
    print(f"ERROR: Data file not found: {DATA_PATH.resolve()}")
    sys.exit(1)

# ==============
# 1. LOAD RAW DATA (Excel .xls)
# ==============
# use xlrd engine for .xls files
print("Reading Excel file:", DATA_PATH)
df = pd.read_excel(DATA_PATH, engine="xlrd")

print("Raw shape:", df.shape)
print("Columns:", df.columns.tolist())

# ==============
# 1.1 Normalize column names to safe snake_case (lowercase)
# ==============
def normalize_cols(cols):
    return [
        str(c).strip().lower().replace(" ", "_").replace("-", "_")
        for c in cols
    ]

df.columns = normalize_cols(df.columns)
print("Normalized columns:", df.columns.tolist())

# Helpful: show mapping of typical expected columns vs available
expected_candidates = {
    'order_id': ['order_id', 'order_id'],
    'order_date': ['order_date', 'orderdate', 'order_date'],
    'ship_date': ['ship_date', 'shipdate'],
    'ship_mode': ['ship_mode', 'shipmode'],
    'customer_id': ['customer_id', 'customerid'],
    'customer_name': ['customer_name', 'customername', 'customer'],
    'segment': ['segment'],
    'country': ['country'],
    'city': ['city'],
    'state': ['state'],
    'region': ['region'],
    'postal_code': ['postal_code', 'postalcode', 'zip', 'zipcode'],
    'product_id': ['product_id', 'productid'],
    'product_name': ['product_name', 'productname', 'product'],
    'category': ['category'],
    'sub_category': ['sub_category', 'sub-category', 'subcategory'],
    'sales': ['sales', 'sales_amount'],
    'quantity': ['quantity', 'qty'],
    'discount': ['discount'],
    'profit': ['profit']
}

# create a mapping from expected -> actual column in df (if present)
col_map = {}
for logical, candidates in expected_candidates.items():
    found = None
    for c in candidates:
        if c in df.columns:
            found = c
            break
    col_map[logical] = found

print("Detected column map (logical -> actual):")
for k, v in col_map.items():
    print(f"  {k:12} -> {v}")

# ==============
# 2. Keep / rename the columns we will use (if they exist)
# ==============
# Build a working DataFrame that contains at least the columns we need, copying from df if present.
work = pd.DataFrame()

# helper to copy column if exists, else fill NaN
def copy_col(logical_name, target_name=None):
    actual = col_map.get(logical_name)
    if target_name is None:
        target_name = logical_name
    if actual is not None:
        work[target_name] = df[actual].copy()
    else:
        work[target_name] = np.nan

# copy important fields
copy_col('order_id')
copy_col('order_date')
copy_col('ship_date')
copy_col('ship_mode')
copy_col('customer_id')
copy_col('customer_name')
copy_col('segment')
copy_col('country')
copy_col('city')
copy_col('state')
copy_col('region')
copy_col('postal_code')
copy_col('product_id')
copy_col('product_name')
copy_col('category')
copy_col('sub_category')
copy_col('sales')        # monetary value
copy_col('quantity')
copy_col('discount')
copy_col('profit')

# If order_date is not parsed as datetime, attempt parsing
if not pd.api.types.is_datetime64_any_dtype(work['order_date']):
    work['order_date'] = pd.to_datetime(work['order_date'], errors='coerce')
if not pd.api.types.is_datetime64_any_dtype(work['ship_date']):
    work['ship_date'] = pd.to_datetime(work['ship_date'], errors='coerce')

# Convert numeric columns
for numcol in ['sales', 'quantity', 'discount', 'profit']:
    if numcol in work.columns:
        work[numcol] = pd.to_numeric(work[numcol], errors='coerce')

# Drop rows where critical keys are missing (order_id or customer_id or product_id or order_date)
initial_shape = work.shape
work = work.dropna(subset=['order_id', 'customer_id', 'product_id', 'order_date'])
print(f"Dropped rows missing critical keys: from {initial_shape} -> {work.shape}")

# remove rows with zero or negative quantity
if 'quantity' in work.columns:
    work = work[work['quantity'].fillna(0) > 0]

# ==============
# 3. DIMENSIONS
# ==============
# dim_customer
dim_customer = work[[
    'customer_id', 'customer_name', 'segment',
    'country', 'city', 'state', 'region', 'postal_code'
]].drop_duplicates().reset_index(drop=True)

# ensure column names for DB
dim_customer = dim_customer.rename(columns={
    'customer_id': 'customer_id',
    'customer_name': 'customer_name',
    'postal_code': 'postal_code'
})

# dim_product
dim_product = work[[
    'product_id', 'product_name', 'category', 'sub_category'
]].drop_duplicates().reset_index(drop=True)

# dim_date: generate date dimension covering data range
min_date = work['order_date'].min()
max_date = work['order_date'].max()
date_range = pd.date_range(start=min_date, end=max_date, freq='D')
dim_date = pd.DataFrame({'date': date_range})
dim_date['date_key'] = dim_date['date'].dt.strftime('%Y%m%d').astype(int)
dim_date['day'] = dim_date['date'].dt.day
dim_date['month'] = dim_date['date'].dt.month
dim_date['month_name'] = dim_date['date'].dt.strftime('%b')
dim_date['year'] = dim_date['date'].dt.year
dim_date['quarter'] = dim_date['date'].dt.quarter
dim_date['day_of_week'] = dim_date['date'].dt.dayofweek
dim_date['day_name'] = dim_date['date'].dt.day_name()
dim_date['is_weekend'] = dim_date['day_of_week'].isin([5,6]).astype(int)

# ==============
# 4. FACT TABLE
# ==============
fact_sales = work.copy()

# make a consistent order_date_key
fact_sales['order_date_key'] = fact_sales['order_date'].dt.strftime('%Y%m%d').astype(int)

# rename to standard names
fact_sales = fact_sales.rename(columns={
    'order_id': 'order_id',
    'order_date': 'order_date',
    'ship_date': 'ship_date',
    'ship_mode': 'ship_mode',
    'customer_id': 'customer_id',
    'product_id': 'product_id',
    'country': 'country',
    'city': 'city',
    'state': 'state',
    'region': 'region',
    'quantity': 'quantity',
    'sales': 'sales_amount',
    'discount': 'discount',
    'profit': 'profit'
})

# surrogate primary key for fact
fact_sales = fact_sales.reset_index(drop=True)
fact_sales['sales_id'] = fact_sales.index + 1

# derived fields
fact_sales['gross_sales'] = fact_sales['sales_amount'].fillna(0) + fact_sales['discount'].fillna(0)
fact_sales['discount_pct'] = np.where(
    fact_sales['gross_sales'] > 0,
    fact_sales['discount'].fillna(0) / fact_sales['gross_sales'],
    0
)

# Keep only the columns we want (if they exist in DataFrame)
keep_cols = [
    'sales_id', 'order_id', 'order_date', 'order_date_key',
    'ship_date', 'ship_mode',
    'customer_id', 'product_id',
    'country', 'city', 'state', 'region',
    'quantity', 'sales_amount', 'gross_sales',
    'discount', 'discount_pct', 'profit'
]

# filter to existing columns
keep_existing = [c for c in keep_cols if c in fact_sales.columns]
fact_sales = fact_sales[keep_existing].copy()

print("Final fact_sales shape:", fact_sales.shape)
print("fact_sales columns:", fact_sales.columns.tolist())

# ==============
# 5. DERIVED METRICS
# ==============
# customer_metrics
# ensure order_id exists in fact_sales for counting; if not, use sales_id for counts
order_id_col = 'order_id' if 'order_id' in fact_sales.columns else 'sales_id'

customer_metrics = fact_sales.groupby('customer_id').agg(
    total_revenue=('sales_amount', 'sum'),
    total_profit=('profit', 'sum'),
    total_orders=(order_id_col, 'nunique' if order_id_col in fact_sales.columns else 'count'),
    total_quantity=('quantity', 'sum'),
    first_order_date=('order_date', 'min'),
    last_order_date=('order_date', 'max')
).reset_index()

max_order_date = fact_sales['order_date'].max() if 'order_date' in fact_sales.columns else pd.Timestamp.now()
customer_metrics['days_since_last_order'] = (max_order_date - customer_metrics['last_order_date']).dt.days
customer_metrics['avg_order_value'] = np.where(
    customer_metrics['total_orders'] > 0,
    customer_metrics['total_revenue'] / customer_metrics['total_orders'],
    0
)

# RFM-like scoring (simple)
def score_from_quantiles(series):
    q = series.quantile([0.25, 0.5, 0.75]).to_dict()
    def scorer(x):
        if x <= q[0.25]:
            return 1
        elif x <= q[0.50]:
            return 2
        elif x <= q[0.75]:
            return 3
        else:
            return 4
    return series.apply(scorer)

# Recency = days_since_last_order (lower better -> invert)
if 'days_since_last_order' in customer_metrics.columns:
    r_score = score_from_quantiles(customer_metrics['days_since_last_order'])
    customer_metrics['R'] = 5 - r_score
else:
    customer_metrics['R'] = 1

customer_metrics['F'] = score_from_quantiles(customer_metrics['total_orders'])
customer_metrics['M'] = score_from_quantiles(customer_metrics['total_revenue'])
customer_metrics['RFM_score'] = customer_metrics['R'].astype(str) + customer_metrics['F'].astype(str) + customer_metrics['M'].astype(str)

# monthly_sales
if 'order_date' in fact_sales.columns and 'sales_amount' in fact_sales.columns:
    fact_sales['year'] = fact_sales['order_date'].dt.year
    fact_sales['month'] = fact_sales['order_date'].dt.month
    monthly_sales = fact_sales.groupby(['year', 'month']).agg(
        monthly_revenue=('sales_amount', 'sum'),
        monthly_profit=('profit', 'sum') if 'profit' in fact_sales.columns else ('sales_amount', 'sum'),
        total_orders=(order_id_col, 'nunique' if order_id_col in fact_sales.columns else 'count')
    ).reset_index().sort_values(['year', 'month'])
    monthly_sales['cumulative_revenue'] = monthly_sales['monthly_revenue'].cumsum()
else:
    monthly_sales = pd.DataFrame()

# ==============
# 6. WRITE TO SQLITE
# ==============
engine = create_engine(f"sqlite:///{DB_PATH}", echo=False)

# safe write if frame is not empty
def to_sql_safe(df_obj, name):
    if df_obj is None:
        return
    if isinstance(df_obj, pd.DataFrame) and not df_obj.empty:
        df_obj.to_sql(TABLE_PREFIX + name, engine, if_exists='replace', index=False)
        print(f"Written table: {TABLE_PREFIX + name} shape: {df_obj.shape}")
    else:
        print(f"Skipped writing table {TABLE_PREFIX + name} (empty or invalid)")

to_sql_safe(dim_customer, "dim_customer")
to_sql_safe(dim_product, "dim_product")
to_sql_safe(dim_date, "dim_date")
to_sql_safe(fact_sales, "fact_sales")
to_sql_safe(customer_metrics, "customer_metrics")
to_sql_safe(monthly_sales, "monthly_sales")

print("All done. DB path:", DB_PATH.resolve())
print("Available tables:")
from sqlalchemy import text

with engine.connect() as conn:
    result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table';"))
    print(result.fetchall())
