"""
PySpark data quality checks job.
Validates data quality and generates quality reports.
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    when, isnan, isnull, current_timestamp, lit
)

def create_spark_session(app_name="DataQuality"):
    """Create and configure Spark session."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def check_data_completeness(df, table_name="unknown"):
    """Check data completeness metrics."""
    total_records = df.count()
    
    completeness_checks = []
    
    for column in df.columns:
        null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
        completeness_rate = ((total_records - null_count) / total_records) * 100 if total_records > 0 else 0
        
        completeness_checks.append({
            'table_name': table_name,
            'column_name': column,
            'total_records': total_records,
            'null_count': null_count,
            'completeness_rate': completeness_rate,
            'check_type': 'completeness'
        })
    
    return completeness_checks

def check_data_validity(df, table_name="unknown"):
    """Check data validity rules."""
    validity_checks = []
    
    # Check for negative quantities
    if 'quantity' in df.columns:
        negative_quantity = df.filter(col('quantity') < 0).count()
        validity_checks.append({
            'table_name': table_name,
            'check_name': 'negative_quantity',
            'failed_records': negative_quantity,
            'check_type': 'validity',
            'rule': 'quantity >= 0'
        })
    
    # Check for negative prices
    if 'price' in df.columns:
        negative_price = df.filter(col('price') < 0).count()
        validity_checks.append({
            'table_name': table_name,
            'check_name': 'negative_price',
            'failed_records': negative_price,
            'check_type': 'validity',
            'rule': 'price >= 0'
        })
    
    # Check for future dates
    if 'transaction_date' in df.columns:
        future_dates = df.filter(col('transaction_date') > current_timestamp()).count()
        validity_checks.append({
            'table_name': table_name,
            'check_name': 'future_transaction_date',
            'failed_records': future_dates,
            'check_type': 'validity',
            'rule': 'transaction_date <= current_date'
        })
    
    return validity_checks

def check_data_consistency(df, table_name="unknown"):
    """Check data consistency rules."""
    consistency_checks = []
    
    # Check total_amount calculation consistency
    if all(col in df.columns for col in ['quantity', 'price', 'total_amount']):
        inconsistent_amounts = df.filter(
            abs(col('total_amount') - (col('quantity') * col('price'))) > 0.01
        ).count()
        
        consistency_checks.append({
            'table_name': table_name,
            'check_name': 'total_amount_calculation',
            'failed_records': inconsistent_amounts,
            'check_type': 'consistency',
            'rule': 'total_amount = quantity * price'
        })
    
    return consistency_checks

def check_referential_integrity(df, table_name="unknown"):
    """Check referential integrity."""
    integrity_checks = []
    
    # Check for duplicate IDs
    if 'id' in df.columns:
        total_records = df.count()
        unique_ids = df.select('id').distinct().count()
        duplicate_ids = total_records - unique_ids
        
        integrity_checks.append({
            'table_name': table_name,
            'check_name': 'duplicate_ids',
            'failed_records': duplicate_ids,
            'check_type': 'integrity',
            'rule': 'id should be unique'
        })
    
    return integrity_checks

def generate_data_profile(df, table_name="unknown"):
    """Generate data profiling statistics."""
    total_records = df.count()
    
    profile = {
        'table_name': table_name,
        'total_records': total_records,
        'column_count': len(df.columns),
        'timestamp': current_timestamp()
    }
    
    # Numeric column statistics
    numeric_cols = [col_name for col_name, col_type in df.dtypes 
                   if col_type in ['int', 'bigint', 'float', 'double']]
    
    for col_name in numeric_cols:
        stats = df.select(
            spark_min(col(col_name)).alias('min_val'),
            spark_max(col(col_name)).alias('max_val'),
            avg(col(col_name)).alias('avg_val'),
            count(col(col_name)).alias('non_null_count')
        ).collect()[0]
        
        profile[f'{col_name}_min'] = stats['min_val']
        profile[f'{col_name}_max'] = stats['max_val']
        profile[f'{col_name}_avg'] = stats['avg_val']
        profile[f'{col_name}_non_null_count'] = stats['non_null_count']
    
    return profile

def main():
    """Main function for data quality checks."""
    parser = argparse.ArgumentParser(description='PySpark Data Quality Job')
    parser.add_argument('--input-path', required=True, help='Input data path')
    parser.add_argument('--date', required=True, help='Processing date')
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session("DataQuality")
    
    try:
        print(f"Starting data quality checks for date: {args.date}")
        print(f"Input path: {args.input_path}")
        
        all_quality_results = []
        all_profiles = []
        
        # Check all datasets
        datasets = [
            ('transformed_sales', f"{args.input_path}/transformed_sales"),
            ('daily_sales_summary', f"{args.input_path}/daily_sales_summary"),
            ('customer_summary', f"{args.input_path}/customer_summary"),
            ('product_summary', f"{args.input_path}/product_summary")
        ]
        
        for table_name, path in datasets:
            try:
                print(f"\nProcessing quality checks for: {table_name}")
                df = spark.read.parquet(path)
                
                # Run all quality checks
                completeness = check_data_completeness(df, table_name)
                validity = check_data_validity(df, table_name)
                consistency = check_data_consistency(df, table_name)
                integrity = check_referential_integrity(df, table_name)
                
                # Generate data profile
                profile = generate_data_profile(df, table_name)
                all_profiles.append(profile)
                
                # Combine all checks
                all_checks = completeness + validity + consistency + integrity
                all_quality_results.extend(all_checks)
                
                print(f"Completed quality checks for {table_name}: {len(all_checks)} checks performed")
                
            except Exception as e:
                print(f"Warning: Could not process {table_name}: {str(e)}")
                continue
        
        # Convert results to DataFrames and save
        if all_quality_results:
            # Quality check results
            quality_df = spark.createDataFrame(all_quality_results)
            quality_df = quality_df.withColumn("check_timestamp", current_timestamp())
            quality_df = quality_df.withColumn("processing_date", lit(args.date))
            
            quality_output_path = f"{args.input_path}/quality_results"
            quality_df.write.mode("overwrite").parquet(quality_output_path)
            print(f"Quality results saved to: {quality_output_path}")
            
            # Print summary
            failed_checks = quality_df.filter(
                (col("failed_records") > 0) | (col("completeness_rate") < 95)
            ).count()
            
            print(f"\nQuality Check Summary:")
            print(f"Total checks performed: {len(all_quality_results)}")
            print(f"Failed checks: {failed_checks}")
            
            if failed_checks > 0:
                print("Failed checks details:")
                quality_df.filter(
                    (col("failed_records") > 0) | (col("completeness_rate") < 95)
                ).show(20, False)
                
                # Fail the job if critical issues found
                critical_failures = quality_df.filter(
                    col("failed_records") > 100  # Threshold for critical failures
                ).count()
                
                if critical_failures > 0:
                    raise Exception(f"Critical data quality issues found: {critical_failures} checks failed")
        
        # Save data profiles
        if all_profiles:
            profiles_df = spark.createDataFrame(all_profiles)
            profiles_output_path = f"{args.input_path}/data_profiles"
            profiles_df.write.mode("overwrite").parquet(profiles_output_path)
            print(f"Data profiles saved to: {profiles_output_path}")
        
        print("Data quality checks completed successfully")
        
    except Exception as e:
        print(f"Error in data quality checks: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
