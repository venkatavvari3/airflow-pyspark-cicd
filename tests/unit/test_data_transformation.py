"""
Unit tests for data transformation job.
"""

import pytest
from pyspark.sql.functions import col
from src.jobs.data_transformation import transform_sales_data, create_aggregations

class TestDataTransformation:
    
    def test_transform_sales_data_adds_total_amount(self, sample_data):
        """Test that transform_sales_data adds total_amount column."""
        df_transformed = transform_sales_data(sample_data)
        
        assert "total_amount" in df_transformed.columns
        
        # Check calculation for first row
        first_row = df_transformed.first()
        expected_total = first_row["quantity"] * first_row["price"]
        assert abs(first_row["total_amount"] - expected_total) < 0.01
    
    def test_transform_sales_data_adds_date_components(self, sample_data):
        """Test that transform_sales_data adds date components."""
        df_transformed = transform_sales_data(sample_data)
        
        date_columns = ["year", "month", "day"]
        for col_name in date_columns:
            assert col_name in df_transformed.columns
        
        # Check that values are reasonable
        first_row = df_transformed.first()
        assert first_row["year"] == 2024
        assert 1 <= first_row["month"] <= 12
        assert 1 <= first_row["day"] <= 31
    
    def test_transform_sales_data_categorizes_customers(self, sample_data):
        """Test customer categorization logic."""
        df_transformed = transform_sales_data(sample_data)
        
        assert "customer_total_spent" in df_transformed.columns
        assert "customer_category" in df_transformed.columns
        
        # All categories should be valid
        valid_categories = ["Premium", "Gold", "Silver", "Bronze"]
        categories = df_transformed.select("customer_category").distinct().collect()
        for row in categories:
            assert row["customer_category"] in valid_categories
    
    def test_create_aggregations_structure(self, sample_data):
        """Test that create_aggregations returns correct structure."""
        df_transformed = transform_sales_data(sample_data)
        daily_sales, customer_summary, product_summary = create_aggregations(df_transformed)
        
        # Check daily sales structure
        expected_daily_cols = ["year", "month", "day", "region", "daily_revenue", 
                              "daily_transactions", "avg_transaction_amount"]
        assert set(daily_sales.columns) == set(expected_daily_cols)
        
        # Check customer summary structure
        expected_customer_cols = ["customer_id", "customer_category", "region", 
                                 "total_spent", "total_transactions", 
                                 "avg_transaction_amount", "last_purchase_date"]
        assert set(customer_summary.columns) == set(expected_customer_cols)
        
        # Check product summary structure
        expected_product_cols = ["product_id", "category", "region", 
                               "total_quantity_sold", "total_revenue", 
                               "total_orders", "avg_price"]
        assert set(product_summary.columns) == set(expected_product_cols)
    
    def test_aggregations_data_consistency(self, sample_data):
        """Test data consistency in aggregations."""
        df_transformed = transform_sales_data(sample_data)
        daily_sales, customer_summary, product_summary = create_aggregations(df_transformed)
        
        # Check that aggregations have data
        assert daily_sales.count() > 0
        assert customer_summary.count() > 0
        assert product_summary.count() > 0
        
        # Check that customer counts are reasonable
        total_customers = df_transformed.select("customer_id").distinct().count()
        assert customer_summary.count() >= total_customers  # Could be more due to region splits
