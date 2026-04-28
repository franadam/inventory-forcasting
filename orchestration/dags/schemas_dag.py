from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from orchestration.dag_config import POSTGRES_CONNECTION_ARGS
from spark.common.spark_config import SPARK_CONN_ID
from spark.common.config import SQL_ROOT

with DAG(
    dag_id="schemas_dag",
    default_args=POSTGRES_CONNECTION_ARGS,
    description="DAG to create all the schemas",
    schedule="0 14 * * *",
    catchup=False,
    # add the path to the dag search path -> allow us to use sql file
    template_searchpath=[SQL_ROOT],
    tags=["bronze", "staging", "silver", "gold", "schema", "postgresql"],
) as dag_schemas:

    # Define tasks
    drop_bronze_schema_task = SQLExecuteQueryOperator(
        task_id="drop_bronze_schema",
        sql=f"DROP SCHEMA IF EXISTS bronze CASCADE ;",
        split_statements=False,
        return_last=False,
    )
    drop_silver_schema_task = SQLExecuteQueryOperator(
        task_id="drop_silver_schema",
        sql=f"DROP SCHEMA IF EXISTS silver CASCADE ;",
        split_statements=False,
        return_last=False,
    )
    drop_gold_schema_task = SQLExecuteQueryOperator(
        task_id="drop_gold_schema",
        sql=f"DROP SCHEMA IF EXISTS gold CASCADE ;",
        split_statements=False,
        return_last=False,
    )

    create_bronze_schema_task = SQLExecuteQueryOperator(
        task_id="create_bronze_schema",
        sql=f"CREATE SCHEMA IF NOT EXISTS bronze;",
        split_statements=False,
        return_last=False,
    )

    create_bronze_tables_task = SQLExecuteQueryOperator(
        task_id="create_bronze_tables",
        sql=f"/tables/bronze_tables.sql",
        split_statements=True,
        return_last=False,
    )

    create_staging_schema_task = SQLExecuteQueryOperator(
        task_id="create_staging_schema",
        sql=f"CREATE SCHEMA IF NOT EXISTS staging;",
        split_statements=False,
        return_last=False,
    )

    create_silver_schema_task = SQLExecuteQueryOperator(
        task_id="create_silver_schema",
        sql=f"CREATE SCHEMA IF NOT EXISTS silver;",
        split_statements=False,
        return_last=False,
    )

    create_silver_tables_task = SQLExecuteQueryOperator(
        task_id="create_silver_tables",
        sql=f"/tables/silver_tables.sql",
        split_statements=True,
        return_last=False,
    )

    populate_silver_tables_task = SQLExecuteQueryOperator(
        task_id="populate_silver_tables",
        sql=f"/tables/silver_populate_tables.sql",
        split_statements=True,
        return_last=False,
    )

    create_gold_schema_task = SQLExecuteQueryOperator(
        task_id="create_gold_schema",
        sql=f"CREATE SCHEMA IF NOT EXISTS gold;",
        split_statements=False,
        return_last=False,
    )

    create_gold_tables_task = SQLExecuteQueryOperator(
        task_id="create_gold_tables",
        sql=f"/tables/gold_tables.sql",
        split_statements=True,
        return_last=False,
    )

    truncate_gold_tables_task = SQLExecuteQueryOperator(
        task_id="truncate_gold_tables",
        sql=f"/tables/gold_truncate_tables.sql",
        split_statements=True,
        return_last=False,
    )

    # Define dependencies
    drop_bronze_schema_task >> drop_silver_schema_task >> drop_gold_schema_task >> create_bronze_schema_task >> create_bronze_tables_task >> create_staging_schema_task >> create_silver_schema_task >> create_silver_tables_task >> populate_silver_tables_task >> create_gold_schema_task >> create_gold_tables_task >> truncate_gold_tables_task
