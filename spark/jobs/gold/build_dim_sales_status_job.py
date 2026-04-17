import logging

from pyspark.sql import DataFrame

from spark.common.spark_session import build_spark_session
from spark.common.dataframe_utils import save_into_db
from spark.transformations.gold.dim_sales_status import add_dim_sales_status_surrogate_key, prepare_internal_sales_status, reorder_dim_sales_status_columns

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_dim_sales_status() -> DataFrame:
    spark = build_spark_session("build_dim_sales_status")

    sales_status_df = prepare_internal_sales_status(spark)

    dim_sales_status_df = sales_status_df\
        .transform(add_dim_sales_status_surrogate_key)\
        .transform(reorder_dim_sales_status_columns)

    save_into_db(schema='gold', table='dim_sales_status',
                 dataframe=dim_sales_status_df)

    return dim_sales_status_df


if __name__ == "__main__":
    build_dim_sales_status()
