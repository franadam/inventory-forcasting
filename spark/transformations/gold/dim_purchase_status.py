from itertools import chain

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.common.dataframe_utils import read_postgresql_table
from spark.common.gold_utils import reorder_columns


def prepare_internal_purchase_status(spark: SparkSession) -> DataFrame:
    silver_purchase_df = read_postgresql_table(
        spark=spark, schema="silver", table="purchase_orders")
    purchase_status_df = silver_purchase_df \
        .select("status")\
        .withColumnRenamed("status", "status_name")\
        .withColumn("status_code", F.upper(F.substring(F.col("status_name"), 0, 3)))\
        .dropDuplicates(["status_name"])
    return purchase_status_df


def reorder_dim_purchase_status_columns(df: DataFrame) -> DataFrame:
    ordered_list = ["status_code", "status_name"]
    ordered_df = reorder_columns(df=df, ordered_list=ordered_list)
    return ordered_df
