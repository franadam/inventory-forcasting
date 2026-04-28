import logging

from pyspark.sql import DataFrame

from spark.common.spark_session import build_spark_session
from spark.common.dataframe_utils import save_into_db
from spark.transformations.gold.dim_customer import prepare_internal_customers, reorder_dim_customer_columns


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_dim_customer() -> DataFrame:
    spark = build_spark_session("build_dim_customer")

    customer_df = prepare_internal_customers(spark)

    dim_customer_df = customer_df\
        .transform(reorder_dim_customer_columns)

    save_into_db(schema='gold', table='dim_customer',
                 dataframe=dim_customer_df, mode="append")
                 
    logger.info("Columns in customer_df: %s", customer_df.columns)
    return dim_customer_df


if __name__ == "__main__":
    build_dim_customer()
