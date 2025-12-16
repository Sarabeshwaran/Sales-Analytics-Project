Sales Analytics Dashboard (Python + SQL + Tableau):

This project is an end-to-end Sales Analytics Pipeline built using Python, SQL (SQLite), and Tableau.
The goal is to transform the raw Superstore dataset into a clean analytical model and build an interactive dashboard that provides insights into revenue, profit, customer behavior, and product performance.

Project Overview:

This project demonstrates the full workflow of a Data Analyst:

Data Extraction & Cleaning (Python + Pandas)

Dimensional Data Modeling (Fact & Dimension tables)

Loading into SQLite Database (ETL)

SQL Transformations & Metrics Development

Building an Interactive Tableau Dashboard

Business Insights & Data Storytelling

Technologies Used:

Python (Pandas, SQLite3)

SQL (CTEs, aggregation, joins)

Tableau Public

Excel / CSV

ETL Pipeline (Python):

The ETL script sales_pipeline.py performs:

Loading raw Superstore dataset (XLS)

Cleaning column names

Handling missing data

Creating:

Fact_Sales table

Dim_Customer

Dim_Product

Dim_Date

Generating date keys

Exporting cleaned tables

Loading everything into SQLite database

Tableau Dashboard :

The dashboard includes:

KPI Tiles

Total Revenue
<img width="1170" height="481" alt="image" src="https://github.com/user-attachments/assets/2e4ce310-4c7a-408e-991b-a5f8a695df04" />

Total Profit
<img width="1170" height="480" alt="image" src="https://github.com/user-attachments/assets/52558adc-6a16-4a64-9300-f55bfd24b6e7" />

Total Orders
<img width="1169" height="481" alt="image" src="https://github.com/user-attachments/assets/4d7ce2cb-c637-4e3e-9755-f4cb83d0acb5" />

Monthly Revenue Trend (2014–2017)
<img width="1169" height="480" alt="image" src="https://github.com/user-attachments/assets/73889bb4-7ca6-4080-b261-d5344f9b7a9f" />

Sales by Category
<img width="1171" height="480" alt="image" src="https://github.com/user-attachments/assets/dbdd5482-b028-4bb8-8ec3-d1ba1f152574" />

Sales by State Map
<img width="1171" height="480" alt="image" src="https://github.com/user-attachments/assets/7e2fcf18-8d6d-46ed-bc9a-902645b6a9c2" />

Top 10 Customers by Revenue
<img width="1170" height="478" alt="image" src="https://github.com/user-attachments/assets/9f9ea391-602a-4a44-904e-2db260e59076" />

Product Profitability Scatter Plot
<img width="1171" height="487" alt="image" src="https://github.com/user-attachments/assets/b3f0279d-ba17-462a-9ce5-7ad53e795217" />

Key Business Insights:

Technology category generates the highest revenue.

California, New York, and Texas dominate US sales.

Revenue grows steadily from 2014 to 2017.

Discounts significantly impact profitability in certain categories.

Top 10 customers contribute a large share of overall revenue.

<img width="1366" height="638" alt="image" src="https://github.com/user-attachments/assets/3d0941f9-a05a-4b7c-b526-b3ac463e26ea" />


GitHub for version control# Sales-Analytics-Project
