from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from orchestration.dag_config import DEFAULT_AIRFLOW_ARGS
from spark.common.spark_config import SPARK_CONN_ID
from spark.jobs.bronze.load_bronze_tables_job import create_bronze_tables

with DAG(
    dag_id="bronze_dag",
    default_args=DEFAULT_AIRFLOW_ARGS,
    description="DAG to load raw data into database",
    schedule="0 14 * * *",
    catchup=False,
    tags=["bronze", "spark"],
) as dag_bronze:
    
    # Define tasks
    create_bronze_schema_task = PythonOperator(
        task_id="create_bronze_schema",
        python_callable=create_bronze_tables,
    )

    populate_bronze_tables_task = SparkSubmitOperator(
        task_id="populate_bronze_tables",
        conn_id=SPARK_CONN_ID,
        application="/opt/project/spark/jobs/bronze/load_bronze_tables_job.py",
        name="populate_bronze_tables",
        verbose=True
    )

    # Define dependencies
    create_bronze_schema_task >> populate_bronze_tables_task
