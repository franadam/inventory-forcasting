from itertools import chain

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.common.dataframe_utils import read_postgresql_table
from spark.common.gold_utils import reorder_columns


def prepare_internal_sale_channel(spark: SparkSession) -> DataFrame:
    silver_sales_df = read_postgresql_table(
        spark=spark, schema="silver", table="sales_orders")
    sales_channel_df = silver_sales_df \
        .select("source")\
        .withColumnRenamed("source", "source_name")\
        .withColumn("source_code", F.upper(F.substring(F.col("source_name"), 0, 3)))\
        .dropDuplicates(["source_name"])
    return sales_channel_df


def reorder_dim_sales_channel_columns(df: DataFrame) -> DataFrame:
    ordered_list = ["source_code", "source_name"]
    ordered_df = reorder_columns(df=df, ordered_list=ordered_list)
    return ordered_df
