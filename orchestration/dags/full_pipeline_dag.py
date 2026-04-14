from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from orchestration.dag_config import DEFAULT_AIRFLOW_ARGS

with DAG(
    dag_id="full_pipeline_dag",
    default_args=DEFAULT_AIRFLOW_ARGS,
    description="Trigger Bronze, then Silver, then Gold sequentially",
    schedule="0 14 * * *",
    catchup=False,
    tags=["silver", "full-pipeline", "spark", "bronze", "gold"],
) as dag_full_pipeline:
    
    # Define tasks    
    trigger_schemas_dag_task = TriggerDagRunOperator(
        task_id="trigger_schemas_dag",
        trigger_dag_id="schemas_dag",
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=["success"],
        failed_states=["failed"],
    )
    
    trigger_bronze_dag_task = TriggerDagRunOperator(
        task_id="trigger_bronze_dag",
        trigger_dag_id="bronze_dag",
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    trigger_silver_dag_task = TriggerDagRunOperator(
        task_id="trigger_silver_dag",
        trigger_dag_id="silver_dag",
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    trigger_gold_dag_task = TriggerDagRunOperator(
        task_id="trigger_gold_dag",
        trigger_dag_id="gold_dag",
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    # Define dependencies
    trigger_schemas_dag_task >> trigger_bronze_dag_task >> trigger_silver_dag_task >> trigger_gold_dag_task