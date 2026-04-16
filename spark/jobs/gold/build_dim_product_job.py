import logging

from pyspark.sql import DataFrame

from spark.common.spark_session import build_spark_session
from spark.common.data_loading import save_into_db
from spark.transformations.gold.dim_product import add_dim_product_surrogate_key, prepare_internal_products, reorder_dim_product_columns

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_dim_product() -> DataFrame:
    spark = build_spark_session("build_dim_product")

    product_df = prepare_internal_products(spark)

    dim_product_df = product_df\
        .transform(add_dim_product_surrogate_key)\
        .transform(reorder_dim_product_columns)

    save_into_db(schema='gold', table='dim_product', dataframe=dim_product_df)

    return dim_product_df


if __name__ == "__main__":
    build_dim_product()
