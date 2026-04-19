from itertools import chain

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.common.dataframe_utils import read_postgresql_table
from spark.common.gold_utils import reorder_columns


def prepare_internal_sales_status(spark: SparkSession) -> DataFrame:
    silver_sales_df = read_postgresql_table(
        spark=spark, schema="silver", table="sales_orders")
    sales_status_df = silver_sales_df \
        .select("status")\
        .withColumnRenamed("sales_order_id_source")\
        .withColumn("status_code", F.upper(F.substring(F.col("status_name"), 0, 3)))\
        .dropDuplicates(["status_name"])
    return sales_status_df


def reorder_dim_sales_status_columns(df: DataFrame) -> DataFrame:
    ordered_list = ["status_code", "status_name"]
    ordered_df = reorder_columns(df=df, ordered_list=ordered_list)
    return ordered_df
