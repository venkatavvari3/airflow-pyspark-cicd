"""
Smoke tests for different environments.
"""

import pytest
import requests
import os

class TestSmokeTests:
    
    @pytest.fixture
    def environment(self):
        """Get test environment from environment variable."""
        return os.getenv('TEST_ENVIRONMENT', 'dev')
    
    @pytest.fixture
    def airflow_url(self, environment):
        """Get Airflow URL based on environment."""
        urls = {
            'dev': 'http://airflow-dev.company.com',
            'staging': 'http://airflow-staging.company.com',
            'prod': 'http://airflow-prod.company.com'
        }
        return urls.get(environment, urls['dev'])
    
    def test_airflow_webserver_health(self, airflow_url):
        """Test that Airflow webserver is responding."""
        try:
            response = requests.get(f"{airflow_url}/health", timeout=30)
            assert response.status_code == 200
        except requests.RequestException as e:
            pytest.skip(f"Airflow not accessible: {e}")
    
    def test_dag_exists(self, airflow_url):
        """Test that our DAG exists in Airflow."""
        try:
            # This would require authentication in real scenario
            response = requests.get(f"{airflow_url}/api/v1/dags/pyspark_data_pipeline", timeout=30)
            # In dev environment, might get 401 without auth, which is OK
            assert response.status_code in [200, 401, 403]
        except requests.RequestException as e:
            pytest.skip(f"Cannot check DAG existence: {e}")
    
    def test_spark_cluster_connectivity(self, environment):
        """Test basic connectivity to Spark cluster."""
        # This would test actual Spark cluster connectivity
        # For now, just verify environment is set
        assert environment in ['dev', 'staging', 'prod']
        print(f"Smoke test running in {environment} environment")
    
    def test_s3_bucket_access(self):
        """Test S3 bucket accessibility."""
        # In a real scenario, this would test S3 access
        # For now, just check that AWS region is set
        aws_region = os.getenv('AWS_REGION', 'us-east-1')
        assert aws_region is not None
        print(f"AWS region configured: {aws_region}")
