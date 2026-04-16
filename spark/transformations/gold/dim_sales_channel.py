from itertools import chain

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.common.data_loading import read_postgresql_table
from spark.common.gold_utils import reorder_columns, add_surrogate_key, generate_code_helper


def prepare_internal_sale_channel(spark: SparkSession) -> DataFrame:
    silver_sales_df = read_postgresql_table(
        spark=spark, schema="silver", table="sales_orders")
    sales_channel_df = silver_sales_df \
        .select("order_id", "source")\
        .withColumnRenamed("order_id", "sales_order_id_source")\
        .withColumnRenamed("source", "source_name")\
        .dropDuplicates(["source_name"])
    return sales_channel_df


def add_dim_sales_channel_surrogate_key(df: DataFrame) -> DataFrame:
    order_by_list = ["sales_order_id_source", "source_name"]
    cleaned_df = add_surrogate_key(df=df, surrogate_key="SK_dim_sales_channel", ordered_list=order_by_list)\
        .withColumn("source_code", generate_code_helper(F.col("source_name"), F.col("SK_dim_sales_channel")))
    return cleaned_df


def reorder_dim_sales_channel_columns(df: DataFrame) -> DataFrame:
    ordered_list = ["SK_dim_sales_channel",
                    "sales_order_id_source", "source_code", "source_name"]
    ordered_df = reorder_columns(df=df, ordered_list=ordered_list)
    return ordered_df
