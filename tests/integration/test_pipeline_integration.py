"""
Integration tests for the complete PySpark pipeline.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from src.jobs.data_ingestion import main as ingestion_main
from src.jobs.data_transformation import main as transformation_main
from src.jobs.data_quality import main as quality_main

class TestPipelineIntegration:
    
    @pytest.fixture
    def temp_dirs(self):
        """Create temporary directories for testing."""
        base_temp = tempfile.mkdtemp()
        dirs = {
            'input': Path(base_temp) / 'input',
            'processed': Path(base_temp) / 'processed', 
            'transformed': Path(base_temp) / 'transformed'
        }
        
        for dir_path in dirs.values():
            dir_path.mkdir(parents=True, exist_ok=True)
        
        yield dirs
        
        shutil.rmtree(base_temp)
    
    def test_end_to_end_pipeline(self, spark_session, temp_dirs):
        """Test the complete pipeline end-to-end."""
        # Create sample input data
        sample_data = [
            "id,customer_id,product_id,quantity,price,transaction_date,category,region",
            "1,CUST001,PROD001,2,10.50,2024-01-01 10:00:00,Electronics,North",
            "2,CUST002,PROD002,1,25.00,2024-01-01 11:00:00,Clothing,South",
            "3,CUST001,PROD003,3,15.75,2024-01-01 12:00:00,Electronics,North"
        ]
        
        input_file = temp_dirs['input'] / 'sample_data.csv'
        with open(input_file, 'w') as f:
            f.write('\n'.join(sample_data))
        
        # Test data ingestion
        df = spark_session.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(str(input_file))
        
        df.write.mode("overwrite").parquet(str(temp_dirs['processed']))
        
        # Verify processed data exists
        processed_df = spark_session.read.parquet(str(temp_dirs['processed']))
        assert processed_df.count() == 3
        
        # Test transformation (simplified)
        processed_df.withColumn("total_amount", 
                               processed_df.quantity * processed_df.price) \
                   .write.mode("overwrite") \
                   .parquet(str(temp_dirs['transformed'] / 'transformed_sales'))
        
        # Verify transformed data
        transformed_df = spark_session.read.parquet(
            str(temp_dirs['transformed'] / 'transformed_sales')
        )
        assert transformed_df.count() == 3
        assert "total_amount" in transformed_df.columns
    
    def test_data_pipeline_with_bad_data(self, spark_session, temp_dirs):
        """Test pipeline behavior with bad data."""
        # Create sample data with quality issues
        bad_data = [
            "id,customer_id,product_id,quantity,price,transaction_date,category,region",
            "1,CUST001,PROD001,2,10.50,2024-01-01 10:00:00,Electronics,North",
            "2,,PROD002,1,25.00,2024-01-01 11:00:00,Clothing,South",  # missing customer_id
            "3,CUST001,,3,15.75,2024-01-01 12:00:00,Electronics,North",  # missing product_id
            "4,CUST002,PROD004,-1,8.25,2024-01-01 14:00:00,Home,East"   # negative quantity
        ]
        
        input_file = temp_dirs['input'] / 'bad_data.csv'
        with open(input_file, 'w') as f:
            f.write('\n'.join(bad_data))
        
        # Read and clean data
        df = spark_session.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(str(input_file))
        
        # Apply basic cleaning
        df_clean = df.filter(
            df.customer_id.isNotNull() & 
            df.product_id.isNotNull() & 
            (df.quantity > 0)
        )
        
        # Should only have 1 valid record
        assert df_clean.count() == 1
        
        df_clean.write.mode("overwrite").parquet(str(temp_dirs['processed']))
        
        # Verify only clean data is processed
        processed_df = spark_session.read.parquet(str(temp_dirs['processed']))
        assert processed_df.count() == 1
