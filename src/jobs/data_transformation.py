"""
PySpark data transformation job.
Applies business logic and aggregations to the processed data.
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, max as spark_max, 
    when, date_format, year, month, dayofmonth
)
from pyspark.sql.window import Window

def create_spark_session(app_name="DataTransformation"):
    """Create and configure Spark session."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def transform_sales_data(df):
    """Apply business transformations to sales data."""
    
    # Calculate total amount
    df = df.withColumn("total_amount", col("quantity") * col("price"))
    
    # Add date components
    df = df.withColumn("year", year(col("transaction_date"))) \
           .withColumn("month", month(col("transaction_date"))) \
           .withColumn("day", dayofmonth(col("transaction_date")))
    
    # Categorize customers by spending
    window_spec = Window.partitionBy("customer_id")
    df = df.withColumn("customer_total_spent", 
                       spark_sum("total_amount").over(window_spec))
    
    df = df.withColumn("customer_category",
                       when(col("customer_total_spent") > 10000, "Premium")
                       .when(col("customer_total_spent") > 5000, "Gold")
                       .when(col("customer_total_spent") > 1000, "Silver")
                       .otherwise("Bronze"))
    
    return df

def create_aggregations(df):
    """Create various aggregations for reporting."""
    
    # Daily sales summary
    daily_sales = df.groupBy("year", "month", "day", "region") \
        .agg(
            spark_sum("total_amount").alias("daily_revenue"),
            count("id").alias("daily_transactions"),
            avg("total_amount").alias("avg_transaction_amount")
        )
    
    # Customer summary
    customer_summary = df.groupBy("customer_id", "customer_category", "region") \
        .agg(
            spark_sum("total_amount").alias("total_spent"),
            count("id").alias("total_transactions"),
            avg("total_amount").alias("avg_transaction_amount"),
            spark_max("transaction_date").alias("last_purchase_date")
        )
    
    # Product performance
    product_summary = df.groupBy("product_id", "category", "region") \
        .agg(
            spark_sum("quantity").alias("total_quantity_sold"),
            spark_sum("total_amount").alias("total_revenue"),
            count("id").alias("total_orders"),
            avg("price").alias("avg_price")
        )
    
    return daily_sales, customer_summary, product_summary

def main():
    """Main function for data transformation."""
    parser = argparse.ArgumentParser(description='PySpark Data Transformation Job')
    parser.add_argument('--input-path', required=True, help='Input data path')
    parser.add_argument('--output-path', required=True, help='Output data path')
    parser.add_argument('--date', required=True, help='Processing date')
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session("DataTransformation")
    
    try:
        print(f"Starting data transformation for date: {args.date}")
        print(f"Input path: {args.input_path}")
        print(f"Output path: {args.output_path}")
        
        # Read processed data
        df = spark.read.parquet(args.input_path)
        
        print(f"Input records count: {df.count()}")
        
        # Apply transformations
        df_transformed = transform_sales_data(df)
        
        # Create aggregations
        daily_sales, customer_summary, product_summary = create_aggregations(df_transformed)
        
        # Write transformed data
        print("Writing transformed data...")
        
        # Main transformed dataset
        df_transformed.write \
            .mode("overwrite") \
            .partitionBy("region", "year", "month") \
            .parquet(f"{args.output_path}/transformed_sales")
        
        # Aggregated datasets
        daily_sales.write \
            .mode("overwrite") \
            .partitionBy("region", "year", "month") \
            .parquet(f"{args.output_path}/daily_sales_summary")
        
        customer_summary.write \
            .mode("overwrite") \
            .partitionBy("region") \
            .parquet(f"{args.output_path}/customer_summary")
        
        product_summary.write \
            .mode("overwrite") \
            .partitionBy("region") \
            .parquet(f"{args.output_path}/product_summary")
        
        print("Data transformation completed successfully")
        
        # Print summary statistics
        print(f"Transformed records: {df_transformed.count()}")
        print(f"Daily sales records: {daily_sales.count()}")
        print(f"Customer summary records: {customer_summary.count()}")
        print(f"Product summary records: {product_summary.count()}")
        
    except Exception as e:
        print(f"Error in data transformation: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
