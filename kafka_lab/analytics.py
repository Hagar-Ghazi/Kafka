import duckdb
import os

print("📊 Connecting to data lake via serverless DuckDB Engine...")

# Define paths for both streaming tables
fraud_pattern = "./data_lake/fraud_batch_*.parquet"
sales_pattern = "./data_lake/sales_batch_*.parquet"

# Check what files are currently available on disk
all_files = os.listdir("./data_lake") if os.path.exists("./data_lake") else []
fraud_files = [f for f in all_files if f.startswith("fraud_batch_")]
sales_files = [f for f in all_files if f.startswith("sales_batch_")]

print(f"🔍 Found {len(fraud_files)} Fraud data files and {len(sales_files)} Sales data files in data lake.\n")

# ==========================================
# REPORT 1: REAL-TIME FRAUD METRICS REPORT
# ==========================================
if fraud_files:
    fraud_query = f"""
        SELECT 
            location,
            COUNT(*) as total_fraud_incidents,
            ROUND(AVG(amount), 2) as average_fraud_amount,
            ROUND(MAX(amount), 2) as maximum_fraud_amount
        FROM read_parquet('{fraud_pattern}')
        GROUP BY location;
    """
    print("🚨 [REPORT 1] ANALYTICAL METRICS REPORT (FRAUD):")
    duckdb.sql(fraud_query).show()
else:
    print("⚠️ [REPORT 1] No fraud Parquet files found on disk yet.")

print("-" * 60) # Visual separator

# ==========================================
# REPORT 2: ENFORCED AVRO SALES REPORT
# ==========================================
if sales_files:
    sales_query = f"""
        SELECT 
            item_name,
            COUNT(*) as total_orders_placed,
            ROUND(SUM(price), 2) as total_revenue_generated,
            ROUND(AVG(price), 2) as average_item_price
        FROM read_parquet('{sales_pattern}')
        GROUP BY item_name
        ORDER BY total_revenue_generated DESC;
    """
    print("🏆 [REPORT 2] SALES PERFORMANCE REPORT (AVRO):")
    duckdb.sql(sales_query).show()
else:
    print("⚠️ [REPORT 2] No sales Parquet files found. Run consumer_avro_to_parquet.py first!")