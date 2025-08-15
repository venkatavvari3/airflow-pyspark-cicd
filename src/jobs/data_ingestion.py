"""
PySpark data ingestion job.
Reads raw data from source and performs initial processing.
"""

import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan, isnull, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

def create_spark_session(app_name="DataIngestion"):
    """Create and configure Spark session."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def define_schema():
    """Define the schema for input data."""
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("transaction_date", TimestampType(), True),
        StructField("category", StringType(), True),
        StructField("region", StringType(), True)
    ])

def clean_data(df):
    """Clean and validate input data."""
    # Remove rows with null customer_id or product_id
    df_clean = df.filter(
        col("customer_id").isNotNull() & 
        col("product_id").isNotNull() &
        col("quantity") > 0 &
        col("price") > 0
    )
    
    # Handle missing values
    df_clean = df_clean.fillna({
        "category": "UNKNOWN",
        "region": "UNKNOWN"
    })
    
    # Add processing timestamp
    df_clean = df_clean.withColumn("processed_at", current_timestamp())
    
    return df_clean

def main():
    """Main function for data ingestion."""
    parser = argparse.ArgumentParser(description='PySpark Data Ingestion Job')
    parser.add_argument('--input-path', required=True, help='Input data path')
    parser.add_argument('--output-path', required=True, help='Output data path')
    parser.add_argument('--date', required=True, help='Processing date')
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session("DataIngestion")
    
    try:
        print(f"Starting data ingestion for date: {args.date}")
        print(f"Input path: {args.input_path}")
        print(f"Output path: {args.output_path}")
        
        # Define schema
        schema = define_schema()
        
        # Read input data
        df = spark.read \
            .schema(schema) \
            .option("header", "true") \
            .csv(args.input_path)
        
        print(f"Input records count: {df.count()}")
        
        # Clean data
        df_clean = clean_data(df)
        
        print(f"Clean records count: {df_clean.count()}")
        
        # Write processed data
        df_clean.write \
            .mode("overwrite") \
            .partitionBy("region") \
            .parquet(args.output_path)
        
        print("Data ingestion completed successfully")
        
    except Exception as e:
        print(f"Error in data ingestion: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
