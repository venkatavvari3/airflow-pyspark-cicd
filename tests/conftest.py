"""
Test configuration for different environments.
"""

import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for testing."""
    spark = SparkSession.builder \
        .appName("PySpark-Tests") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .config("spark.sql.shuffle.partitions", "2") \
        .master("local[2]") \
        .getOrCreate()
    
    yield spark
    spark.stop()

@pytest.fixture
def sample_data(spark_session):
    """Create sample data for testing."""
    data = [
        (1, "CUST001", "PROD001", 2, 10.50, "2024-01-01 10:00:00", "Electronics", "North"),
        (2, "CUST002", "PROD002", 1, 25.00, "2024-01-01 11:00:00", "Clothing", "South"),
        (3, "CUST001", "PROD003", 3, 15.75, "2024-01-01 12:00:00", "Electronics", "North"),
        (4, "CUST003", "PROD001", 1, 10.50, "2024-01-01 13:00:00", "Electronics", "East"),
        (5, "CUST002", "PROD004", 2, 8.25, "2024-01-01 14:00:00", "Home", "South")
    ]
    
    columns = ["id", "customer_id", "product_id", "quantity", "price", 
               "transaction_date", "category", "region"]
    
    df = spark_session.createDataFrame(data, columns)
    df = df.withColumn("transaction_date", 
                       df.transaction_date.cast("timestamp"))
    
    return df
