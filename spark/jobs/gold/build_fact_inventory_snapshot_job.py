import logging

from pyspark.sql import DataFrame

from spark.common.spark_session import build_spark_session
from spark.common.dataframe_utils import save_into_db
from spark.transformations.gold.fact_inventory_snapshot import prepare_fact_inventory_snapshot, validate_fact_inventory_snapshot, reorder_fact_inventory_snapshot_columns

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_fact_inventory_snapshot() -> DataFrame:
    spark = build_spark_session("build_fact_inventory_snapshot")

    fact_inventory_snapshot_df = prepare_fact_inventory_snapshot(spark)
    fact_inventory_snapshot_df = validate_fact_inventory_snapshot(fact_inventory_snapshot_df)
    fact_inventory_snapshot_df = reorder_fact_inventory_snapshot_columns(fact_inventory_snapshot_df)

    save_into_db(schema='gold', table='fact_inventory_snapshot',
                 dataframe=fact_inventory_snapshot_df, mode="append")

    return fact_inventory_snapshot_df


if __name__ == "__main__":
    build_fact_inventory_snapshot()
