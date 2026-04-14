from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from orchestration.dag_config import DEFAULT_AIRFLOW_ARGS
from spark.common.spark_config import SPARK_CONN_ID

with DAG(
    dag_id="gold_dag",
    default_args=DEFAULT_AIRFLOW_ARGS,
    description="DAG to build the star schema and save it into database",
    schedule="0 14 * * *",
    catchup=False,
    tags=["gold", "star", "modelling", "spark"],
) as dag_gold:
    
    # Define tasks
    build_dim_location_task =  SparkSubmitOperator(
        task_id="build_dim_location",
        conn_id=SPARK_CONN_ID,
        application="/opt/project/spark/jobs/gold/build_dim_location_job.py",
        name="build_dim_location",
        verbose=True
    )

    build_dim_customer_task =  SparkSubmitOperator(
        task_id="build_dim_customer",
        conn_id=SPARK_CONN_ID,
        application="/opt/project/spark/jobs/gold/build_dim_customer_job.py",
        name="build_dim_customer",
        verbose=True
    )
    
    # Define dependencies
    [build_dim_location_task >> build_dim_customer_task]