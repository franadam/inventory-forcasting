from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from orchestration.dag_config import DEFAULT_AIRFLOW_ARGS

with DAG(
    dag_id="clean_bronze_tables_dag",
    default_args=DEFAULT_AIRFLOW_ARGS,
    description="DAG to clean customer raw data and save it into database",
    schedule="0 14 * * *",
    catchup=False,
    tags=["silver", "clean", "spark"],
) as dag_silver:
    
    # Define tasks
    clean_customers_task =  SparkSubmitOperator(
        task_id="clean_customers",
        conn_id="spark_default",
        application="/opt/project/spark/jobs/silver/clean_customer_job.py",
        name="clean_customers",
        verbose=True
    )

    # Define dependencies
    clean_customers_task