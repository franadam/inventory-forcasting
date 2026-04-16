import logging

from pyspark.sql import DataFrame

from spark.common.spark_session import build_spark_session
from spark.common.data_loading import save_into_db
from spark.transformations.gold.dim_sales_channel import add_dim_sales_channel_surrogate_key, prepare_internal_sale_channel, reorder_dim_sales_channel_columns

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_dim_sales_channel() -> DataFrame:
    spark = build_spark_session("build_dim_sales_channel")

    sales_channel_df = prepare_internal_sale_channel(spark)

    dim_sales_channel_df = sales_channel_df\
        .transform(add_dim_sales_channel_surrogate_key)\
        .transform(reorder_dim_sales_channel_columns)

    save_into_db(schema='gold', table='dim_sales_channels',
                 dataframe=dim_sales_channel_df)

    return dim_sales_channel_df


if __name__ == "__main__":
    build_dim_sales_channel()
