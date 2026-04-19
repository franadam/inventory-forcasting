import logging

from pyspark.sql import DataFrame

from spark.common.spark_session import build_spark_session
from spark.common.dataframe_utils import save_into_db
from spark.transformations.gold.dim_purchase_status import prepare_internal_purchase_status, reorder_dim_purchase_status_columns

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_dim_purchase_status() -> DataFrame:
    spark = build_spark_session("build_dim_purchase_status")

    purchase_status_df = prepare_internal_purchase_status(spark)

    dim_purchase_status_df = purchase_status_df\
        .transform(reorder_dim_purchase_status_columns)

    save_into_db(schema='gold', table='dim_purchase_status',
                 dataframe=dim_purchase_status_df, mode="append")

    return dim_purchase_status_df


if __name__ == "__main__":
    build_dim_purchase_status()
