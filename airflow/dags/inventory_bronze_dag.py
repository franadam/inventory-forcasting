from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

from inventory_forcasting.spark.jobs.bronze.load_bronze_tables import create_bronze_tables

# Default Args
default_args = {
    "owner": "franadam",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "franadam.data@engineers.com",
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2025, 1, 1)
}

with DAG(
    dag_id="bronze_dag",
    default_args=default_args,
    description="DAG to load raw data into database",
    schedule="0 14 * * *",
    catchup=False
) as dag_bronze:
    
    # Define tasks
    create_bronze_schema_task = PythonOperator(
        task_id="create_bronze_schema",
        python_callable=create_bronze_tables,
    )

    populate_bronze_tables_task = SparkSubmitOperator(
        task_id="populate_bronze_tables",
        conn_id="spark_default",
        application="/opt/project/spark/jobs/bronze/load_customers_bronze.py",
        name="populate_bronze_tables",
        verbose=True
    )

    # Define dependencies
    create_bronze_schema_task >> populate_bronze_tables_task
