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
    
    clean_suppliers_task = SparkSubmitOperator(
        task_id="clean_suppliers",
        conn_id=SPARK_CONN_ID,
        application="/opt/project/spark/jobs/silver/clean_supplier_job.py",
        name="clean_suppliers",
        verbose=True
    )
    
    clean_inventory_task = SparkSubmitOperator(
        task_id="clean_inventory",
        conn_id=SPARK_CONN_ID,
        application="/opt/project/spark/jobs/silver/clean_inventory_job.py",
        name="clean_inventory",
        verbose=True
    )
    
    clean_inventory_movements_task = SparkSubmitOperator(
        task_id="clean_inventory_movements",
        conn_id=SPARK_CONN_ID,
        application="/opt/project/spark/jobs/silver/clean_inventory_movements_job.py",
        name="clean_inventory_movements",
        verbose=True
    )
    
    clean_product_suppliers_task = SparkSubmitOperator(
        task_id="clean_product_suppliers",
        conn_id=SPARK_CONN_ID,
        application="/opt/project/spark/jobs/silver/clean_product_suppliers_job.py",
        name="clean_product_suppliers",
        verbose=True
    )
    
    clean_purchase_orders_task = SparkSubmitOperator(
        task_id="clean_purchase_orders",
        conn_id=SPARK_CONN_ID,
        application="/opt/project/spark/jobs/silver/clean_purchase_orders_job.py",
        name="clean_purchase_orders",
        verbose=True
    )
    
    clean_purchase_order_lines_task = SparkSubmitOperator(
        task_id="clean_purchase_order_lines",
        conn_id=SPARK_CONN_ID,
        application="/opt/project/spark/jobs/silver/clean_purchase_order_lines_job.py",
        name="clean_purchase_order_lines",
        verbose=True
    )
    
    clean_sales_orders_task = SparkSubmitOperator(
        task_id="clean_sales_orders",
        conn_id=SPARK_CONN_ID,
        application="/opt/project/spark/jobs/silver/clean_sales_orders_job.py",
        name="clean_sales_orders",
        verbose=True
    )

    # Define dependencies
    clean_customers_task >> clean_locations_task >> clean_products_task >> \
    clean_suppliers_task >> clean_inventory_task >> clean_inventory_movements_task >> \
    clean_product_suppliers_task >> clean_purchase_orders_task  >> \
    clean_purchase_order_lines_task >> clean_sales_orders_task