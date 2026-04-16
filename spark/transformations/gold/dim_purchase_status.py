from itertools import chain

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.common.data_loading import read_postgresql_table
from spark.common.gold_utils import reorder_columns, add_surrogate_key, generate_code_helper


def prepare_internal_purchase_status(spark: SparkSession) -> DataFrame:
    silver_purchase_df = read_postgresql_table(
        spark=spark, schema="silver", table="purchase_orders")
    purchase_status_df = silver_purchase_df \
        .select("po_id", "status")\
        .withColumnRenamed("po_id", "purchase_order_id_source")\
        .withColumnRenamed("status", "status_name")\
        .dropDuplicates(["status_name"])
    return purchase_status_df


def add_dim_purchase_status_surrogate_key(df: DataFrame) -> DataFrame:
    order_by_list = ["purchase_order_id_source", "status_name"]
    cleaned_df = add_surrogate_key(df=df, surrogate_key="SK_dim_purchase_status", ordered_list=order_by_list)\
        .withColumn("status_code", generate_code_helper(F.col("status_name"), F.col("SK_dim_purchase_status")))
    return cleaned_df


def reorder_dim_purchase_status_columns(df: DataFrame) -> DataFrame:
    ordered_list = ["SK_dim_purchase_status",
                    "purchase_order_id_source", "status_code", "status_name"]
    ordered_df = reorder_columns(df=df, ordered_list=ordered_list)
    return ordered_df
