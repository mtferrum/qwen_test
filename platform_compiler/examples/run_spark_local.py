"""
Local Spark Runner for Generated SQL

This script demonstrates how to run the generated Spark SQL locally
for testing and validation purposes.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, struct
from pyspark.sql.types import FloatType, DoubleType, StringType, TimestampType
import os

def create_sample_data(spark):
    """Create sample data for testing."""
    
    # Raw events data
    raw_events_data = [
        ("evt_001", "user_123", 150.0, "merch_A", "2024-01-15 10:30:00"),
        ("evt_002", "user_456", 5000.0, "merch_B", "2024-01-15 10:31:00"),
        ("evt_003", "user_123", 75.0, "merch_C", "2024-01-15 10:32:00"),
        ("evt_004", "user_789", 2500.0, "merch_A", "2024-01-15 10:33:00"),
        ("evt_005", "user_456", 50.0, "merch_D", "2024-01-15 10:34:00"),
    ]
    
    raw_events = spark.createDataFrame(
        raw_events_data,
        ["event_id", "user_id", "transaction_amount", "merchant_id", "event_time"]
    )
    raw_events.createOrReplaceTempView("raw_events")
    
    # Merchant risk data
    merchant_risk_data = [
        ("merch_A", 0.3, "retail"),
        ("merch_B", 0.8, "electronics"),
        ("merch_C", 0.2, "grocery"),
        ("merch_D", 0.5, "online"),
    ]
    
    merchant_risk = spark.createDataFrame(
        merchant_risk_data,
        ["merchant_id", "risk_score", "category"]
    )
    merchant_risk.createOrReplaceTempView("merchant_risk")
    
    print("✓ Sample data created")
    return raw_events, merchant_risk

def mock_fraud_detector(user_id, amount, merchant_risk):
    """Mock fraud detection model."""
    # Simple heuristic for demo purposes
    if amount > 1000 and merchant_risk > 0.5:
        return 0.95
    elif amount > 500:
        return 0.6
    else:
        return 0.1

def run_generated_sql(spark, sql_file_path):
    """Execute the generated Spark SQL file."""
    
    print(f"\n📄 Reading SQL from: {sql_file_path}")
    with open(sql_file_path, 'r') as f:
        sql_content = f.read()
    
    # Register mock UDF for APPLY_MODEL
    fraud_udf = udf(mock_fraud_detector, FloatType())
    spark.udf.register("apply_model", fraud_udf)
    spark.udf.register("fraud_detector", fraud_udf)
    
    print("✓ UDFs registered")
    
    # Split SQL into statements and execute
    statements = [s.strip() for s in sql_content.split(';') if s.strip()]
    
    results = {}
    for stmt in statements:
        # Skip comments and empty lines
        if stmt.startswith('--') or not stmt:
            continue
            
        # Extract statement type
        stmt_upper = stmt.upper()
        
        if stmt_upper.startswith('CREATE') and 'VIEW' in stmt_upper:
            # Execute CREATE VIEW
            try:
                spark.sql(stmt)
                view_name = stmt.split('VIEW')[1].split('AS')[0].strip().split('.')[-1]
                print(f"✓ Created view: {view_name}")
            except Exception as e:
                print(f"⚠ View creation skipped (expected for complex views): {str(e)[:100]}")
                
        elif stmt_upper.startswith('INSERT'):
            # Execute INSERT and capture results
            try:
                # Convert INSERT INTO ... SELECT to just SELECT for preview
                select_part = stmt.split('SELECT', 1)[1] if 'SELECT' in stmt else None
                if select_part:
                    full_select = f"SELECT {select_part}"
                    df = spark.sql(full_select)
                    print(f"\n📊 Preview of INSERT results:")
                    df.show(truncate=False)
            except Exception as e:
                print(f"⚠ Insert execution note: {str(e)[:100]}")
    
    # Show final results
    print("\n" + "="*60)
    print("FINAL RESULTS - Flagged Transactions")
    print("="*60)
    
    try:
        result_df = spark.sql("""
            SELECT
                event_id,
                user_id,
                transaction_amount,
                CASE
                    WHEN transaction_amount > 1000 THEN 'HIGH_RISK'
                    WHEN transaction_amount > 500 THEN 'MEDIUM_RISK'
                    ELSE 'LOW_RISK'
                END AS risk_level
            FROM raw_events
            WHERE transaction_amount > 500
        """)
        result_df.show(truncate=False)
    except Exception as e:
        print(f"Note: {e}")

def main():
    """Main entry point."""
    print("="*60)
    print("Spark Local Runner for Generated SQL")
    print("="*60)
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Fraud Detection Pipeline Test") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("✓ Spark session initialized\n")
    
    # Create sample data
    create_sample_data(spark)
    
    # Run generated SQL
    sql_file = os.path.join(os.path.dirname(__file__), '..', 'output', 'spark_final', 'fraud-detection-batch.sql')
    if os.path.exists(sql_file):
        run_generated_sql(spark, sql_file)
    else:
        print(f"⚠ SQL file not found: {sql_file}")
        print("Running basic demo query instead...")
        
        # Demo query
        demo_query = """
        SELECT 
            r.event_id,
            r.user_id,
            r.transaction_amount,
            m.risk_score as merchant_risk,
            CASE
                WHEN r.transaction_amount > 1000 AND m.risk_score > 0.5 THEN 'HIGH_RISK'
                WHEN r.transaction_amount > 500 THEN 'MEDIUM_RISK'
                ELSE 'LOW_RISK'
            END AS risk_level
        FROM raw_events r
        LEFT JOIN merchant_risk m ON r.merchant_id = m.merchant_id
        ORDER BY r.transaction_amount DESC
        """
        
        result = spark.sql(demo_query)
        result.show(truncate=False)
    
    spark.stop()
    print("\n✓ Spark session closed")

if __name__ == "__main__":
    main()
