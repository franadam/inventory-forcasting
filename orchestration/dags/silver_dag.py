from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from orchestration.dag_config import DEFAULT_AIRFLOW_ARGS
from spark.common.spark_config import SPARK_CONN_ID

with DAG(
    dag_id="silver_dag",
    default_args=DEFAULT_AIRFLOW_ARGS,
    description="DAG to clean customer raw data and save it into database",
    schedule="0 14 * * *",
    catchup=False,
    tags=["silver", "clean", "spark"],
) as dag_silver:
    
    # Define tasks
    clean_customers_task =  SparkSubmitOperator(
        task_id="clean_customers",
        conn_id=SPARK_CONN_ID,
        application="/opt/project/spark/jobs/silver/clean_customer_job.py",
        name="clean_customers",
        verbose=True
    )
    
    clean_locations_task = SparkSubmitOperator(
        task_id="clean_locations",
        conn_id=SPARK_CONN_ID,
        application="/opt/project/spark/jobs/silver/clean_location_job.py",
        name="clean_locations",
        verbose=True
    )
    
    clean_products_task = SparkSubmitOperator(
        task_id="clean_products",
        conn_id=SPARK_CONN_ID,
        application="/opt/project/spark/jobs/silver/clean_product_job.py",
        name="clean_products",
        verbose=True
    )

    # Define dependencies
    clean_customers_task >> clean_locations_task >> clean_products_task