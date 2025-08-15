"""
Sample Airflow DAG that triggers PySpark jobs for data processing.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['data-team@company.com']
}

# DAG definition
dag = DAG(
    'pyspark_data_pipeline',
    default_args=default_args,
    description='PySpark data processing pipeline',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['pyspark', 'etl', 'data-pipeline']
)

# EMR cluster configuration
EMR_CLUSTER_CONFIG = {
    'Name': 'pyspark-data-processing-cluster',
    'ReleaseLabel': 'emr-6.15.0',
    'Applications': [{'Name': 'Spark'}, {'Name': 'Hadoop'}],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Master nodes',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
            {
                'Name': 'Worker nodes',
                'Market': 'SPOT',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.large',
                'InstanceCount': 2,
                'BidPrice': '0.10',
            }
        ],
        'Ec2KeyName': 'my-key-pair',
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
    },
    'ServiceRole': 'EMR_DefaultRole',
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'LogUri': 's3://my-emr-logs-bucket/logs/',
}

# PySpark job steps
SPARK_STEPS = [
    {
        'Name': 'Data Ingestion',
        'ActionOnFailure': 'TERMINATE_CLUSTER',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                '--conf', 'spark.sql.adaptive.enabled=true',
                '--conf', 'spark.sql.adaptive.coalescePartitions.enabled=true',
                's3://my-spark-jobs-bucket/jobs/data_ingestion.py',
                '--input-path', 's3://my-data-bucket/raw/{{ ds }}/',
                '--output-path', 's3://my-data-bucket/processed/{{ ds }}/',
                '--date', '{{ ds }}'
            ],
        },
    },
    {
        'Name': 'Data Transformation',
        'ActionOnFailure': 'TERMINATE_CLUSTER',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                '--conf', 'spark.sql.adaptive.enabled=true',
                's3://my-spark-jobs-bucket/jobs/data_transformation.py',
                '--input-path', 's3://my-data-bucket/processed/{{ ds }}/',
                '--output-path', 's3://my-data-bucket/transformed/{{ ds }}/',
                '--date', '{{ ds }}'
            ],
        },
    },
    {
        'Name': 'Data Quality Checks',
        'ActionOnFailure': 'TERMINATE_CLUSTER',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                's3://my-spark-jobs-bucket/jobs/data_quality.py',
                '--input-path', 's3://my-data-bucket/transformed/{{ ds }}/',
                '--date', '{{ ds }}'
            ],
        },
    },
]

def validate_input_data(**context):
    """Validate input data before processing."""
    # Add your data validation logic here
    print(f"Validating input data for {context['ds']}")
    return True

def send_success_notification(**context):
    """Send success notification."""
    print(f"Pipeline completed successfully for {context['ds']}")
    return True

# Task definitions
validate_data = PythonOperator(
    task_id='validate_input_data',
    python_callable=validate_input_data,
    dag=dag
)

create_emr_cluster = EmrCreateJobFlowOperator(
    task_id='create_emr_cluster',
    job_flow_overrides=EMR_CLUSTER_CONFIG,
    aws_conn_id='aws_default',
    dag=dag
)

add_spark_steps = EmrAddStepsOperator(
    task_id='add_spark_steps',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    steps=SPARK_STEPS,
    aws_conn_id='aws_default',
    dag=dag
)

# Sensors for each step
watch_data_ingestion = EmrStepSensor(
    task_id='watch_data_ingestion',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_spark_steps', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)

watch_data_transformation = EmrStepSensor(
    task_id='watch_data_transformation',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_spark_steps', key='return_value')[1] }}",
    aws_conn_id='aws_default',
    dag=dag
)

watch_data_quality = EmrStepSensor(
    task_id='watch_data_quality',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_spark_steps', key='return_value')[2] }}",
    aws_conn_id='aws_default',
    dag=dag
)

terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id='terminate_emr_cluster',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    trigger_rule='all_done',  # Run even if upstream tasks fail
    dag=dag
)

success_notification = PythonOperator(
    task_id='success_notification',
    python_callable=send_success_notification,
    dag=dag
)

failure_notification = EmailOperator(
    task_id='failure_notification',
    to=['data-team@company.com'],
    subject='PySpark Pipeline Failed - {{ ds }}',
    html_content="""
    <h3>PySpark Data Pipeline Failed</h3>
    <p>The PySpark data pipeline failed for date: {{ ds }}</p>
    <p>Please check the logs for more details.</p>
    <p>DAG: {{ dag.dag_id }}</p>
    <p>Task: {{ task_instance.task_id }}</p>
    """,
    trigger_rule='one_failed',
    dag=dag
)

# Task dependencies
validate_data >> create_emr_cluster >> add_spark_steps
add_spark_steps >> watch_data_ingestion >> watch_data_transformation >> watch_data_quality
watch_data_quality >> success_notification >> terminate_emr_cluster
[watch_data_ingestion, watch_data_transformation, watch_data_quality] >> failure_notification >> terminate_emr_cluster
