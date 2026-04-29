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
    build_dim_location_task = SparkSubmitOperator(
        task_id="build_dim_location",
        conn_id=SPARK_CONN_ID,
        application="/opt/project/spark/jobs/gold/build_dim_location_job.py",
        name="build_dim_location",
        verbose=True
    )

    build_dim_customer_task = SparkSubmitOperator(
        task_id="build_dim_customer",
        conn_id=SPARK_CONN_ID,
        application="/opt/project/spark/jobs/gold/build_dim_customer_job.py",
        name="build_dim_customer",
        verbose=True
    )

    build_dim_product_task = SparkSubmitOperator(
        task_id="build_dim_product",
        conn_id=SPARK_CONN_ID,
        application="/opt/project/spark/jobs/gold/build_dim_product_job.py",
        name="build_dim_product",
        verbose=True
    )

    build_dim_supplier_task = SparkSubmitOperator(
        task_id="build_dim_supplier",
        conn_id=SPARK_CONN_ID,
        application="/opt/project/spark/jobs/gold/build_dim_supplier_job.py",
        name="build_dim_supplier",
        verbose=True
    )

    build_dim_date_task = SparkSubmitOperator(
        task_id="build_dim_date",
        conn_id=SPARK_CONN_ID,
        application="/opt/project/spark/jobs/gold/build_dim_date_job.py",
        name="build_dim_date",
        verbose=True
    )

    build_dim_purchase_status_task = SparkSubmitOperator(
        task_id="build_dim_purchase_status",
        conn_id=SPARK_CONN_ID,
        application="/opt/project/spark/jobs/gold/build_dim_purchase_status_job.py",
        name="build_dim_purchase_status",
        verbose=True
    )

    build_dim_sales_channel_task = SparkSubmitOperator(
        task_id="build_dim_sales_channel",
        conn_id=SPARK_CONN_ID,
        application="/opt/project/spark/jobs/gold/build_dim_sales_channel_job.py",
        name="build_dim_sales_channel",
        verbose=True
    )

    build_dim_sales_status_task = SparkSubmitOperator(
        task_id="build_dim_sales_status",
        conn_id=SPARK_CONN_ID,
        application="/opt/project/spark/jobs/gold/build_dim_sales_status_job.py",
        name="build_dim_sales_status",
        verbose=True
    )

    build_fact_sales_line_task = SparkSubmitOperator(
        task_id="build_fact_sales_line",
        conn_id=SPARK_CONN_ID,
        application="/opt/project/spark/jobs/gold/build_fact_sales_line_job.py",
        name="build_fact_sales_line",
        verbose=True
    )

    build_fact_inventory_snapshot_task = SparkSubmitOperator(
        task_id="build_fact_inventory_snapshot",
        conn_id=SPARK_CONN_ID,
        application="/opt/project/spark/jobs/gold/build_fact_inventory_snapshot_job.py",
        name="build_fact_inventory_snapshot",
        verbose=True
    )

    # Define dependencies
    build_dim_date_task >> [
        build_dim_location_task >> build_dim_customer_task >>
        build_dim_product_task >> build_dim_supplier_task >>
        build_dim_purchase_status_task >> build_dim_sales_channel_task >>
        build_dim_sales_status_task
    ] >> build_fact_sales_line_task >> build_fact_inventory_snapshot_task
