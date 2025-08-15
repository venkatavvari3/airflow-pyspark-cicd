"""
Unit tests for data ingestion job.
"""

import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from src.jobs.data_ingestion import clean_data, define_schema

class TestDataIngestion:
    
    def test_define_schema(self):
        """Test schema definition."""
        schema = define_schema()
        assert isinstance(schema, StructType)
        assert len(schema.fields) == 8
        
        field_names = [field.name for field in schema.fields]
        expected_fields = ["id", "customer_id", "product_id", "quantity", 
                          "price", "transaction_date", "category", "region"]
        assert field_names == expected_fields
    
    def test_clean_data_removes_nulls(self, spark_session):
        """Test that clean_data removes null values properly."""
        # Create test data with nulls
        data = [
            (1, "CUST001", "PROD001", 2, 10.50, "Electronics", "North"),
            (2, None, "PROD002", 1, 25.00, "Clothing", "South"),  # null customer_id
            (3, "CUST003", None, 3, 15.75, "Electronics", "East"),  # null product_id
            (4, "CUST004", "PROD004", 0, 8.25, "Home", "West"),    # zero quantity
            (5, "CUST005", "PROD005", 2, -5.00, "Electronics", "North")  # negative price
        ]
        
        columns = ["id", "customer_id", "product_id", "quantity", 
                  "price", "category", "region"]
        
        df = spark_session.createDataFrame(data, columns)
        df_clean = clean_data(df)
        
        # Should only have the first record
        assert df_clean.count() == 1
        assert df_clean.first()["customer_id"] == "CUST001"
    
    def test_clean_data_fills_missing_values(self, spark_session):
        """Test that clean_data fills missing values correctly."""
        data = [
            (1, "CUST001", "PROD001", 2, 10.50, None, None),  # null category and region
        ]
        
        columns = ["id", "customer_id", "product_id", "quantity", 
                  "price", "category", "region"]
        
        df = spark_session.createDataFrame(data, columns)
        df_clean = clean_data(df)
        
        row = df_clean.first()
        assert row["category"] == "UNKNOWN"
        assert row["region"] == "UNKNOWN"
        assert row["processed_at"] is not None
    
    def test_clean_data_preserves_valid_records(self, sample_data):
        """Test that clean_data preserves valid records."""
        df_clean = clean_data(sample_data)
        
        # All sample data should be valid
        assert df_clean.count() == sample_data.count()
        
        # Check that processed_at column is added
        assert "processed_at" in df_clean.columns
