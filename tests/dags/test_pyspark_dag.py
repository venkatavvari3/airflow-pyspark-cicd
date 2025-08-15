"""
Tests for Airflow DAGs.
"""

import pytest
from datetime import datetime, timedelta
from airflow.models import DagBag

class TestDAGs:
    
    def test_dag_loads_successfully(self):
        """Test that DAG loads without errors."""
        dag_bag = DagBag(dag_folder='dags/', include_examples=False)
        
        # Check for import errors
        assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"
        
        # Check that our DAG is loaded
        assert 'pyspark_data_pipeline' in dag_bag.dags
    
    def test_dag_structure(self):
        """Test DAG structure and tasks."""
        dag_bag = DagBag(dag_folder='dags/', include_examples=False)
        dag = dag_bag.get_dag('pyspark_data_pipeline')
        
        # Check DAG properties
        assert dag.schedule_interval == '@daily'
        assert dag.max_active_runs == 1
        assert dag.catchup is False
        
        # Check that expected tasks exist
        expected_tasks = [
            'validate_input_data',
            'create_emr_cluster',
            'add_spark_steps',
            'watch_data_ingestion',
            'watch_data_transformation',
            'watch_data_quality',
            'terminate_emr_cluster',
            'success_notification',
            'failure_notification'
        ]
        
        actual_tasks = list(dag.task_ids)
        for task_id in expected_tasks:
            assert task_id in actual_tasks, f"Task {task_id} not found in DAG"
    
    def test_dag_task_dependencies(self):
        """Test task dependencies are correct."""
        dag_bag = DagBag(dag_folder='dags/', include_examples=False)
        dag = dag_bag.get_dag('pyspark_data_pipeline')
        
        # Check key dependencies
        validate_task = dag.get_task('validate_input_data')
        create_cluster_task = dag.get_task('create_emr_cluster')
        
        # validate_input_data should be upstream of create_emr_cluster
        assert create_cluster_task in validate_task.downstream_list
        
        # terminate_emr_cluster should have trigger_rule 'all_done'
        terminate_task = dag.get_task('terminate_emr_cluster')
        assert terminate_task.trigger_rule == 'all_done'
    
    def test_dag_default_args(self):
        """Test DAG default arguments."""
        dag_bag = DagBag(dag_folder='dags/', include_examples=False)
        dag = dag_bag.get_dag('pyspark_data_pipeline')
        
        # Check default args
        assert dag.default_args['owner'] == 'data-engineering'
        assert dag.default_args['retries'] == 2
        assert dag.default_args['retry_delay'] == timedelta(minutes=5)
        assert dag.default_args['depends_on_past'] is False
