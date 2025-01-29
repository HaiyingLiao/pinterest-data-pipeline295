from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'b82f438a9cb2',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

# Define the DAG
with DAG(
    'b82f438a9cb2_dag',
    default_args=default_args,
    description='A DAG to trigger a Databricks notebook daily',
    schedule_interval='@daily',  # Cron expression for daily execution
    start_date=datetime(2025, 1, 17),  # Set a start date
    catchup=False
) as dag:

    # Task: Trigger Databricks Notebook
    run_notebook = DatabricksSubmitRunOperator(
        task_id='submit_run',
        databricks_conn_id='databricks_default',  # Connection ID configured in Airflow
        existing_cluster_id='1108-162752-8okw8dgg',  # Replace with your Databricks cluster ID
        notebook_task={
            'notebook_path': '/Workspace/Users/haiying1.liao@gmail.com/pinterest',  # Path to your Databricks notebook
        }
    )

    run_notebook
