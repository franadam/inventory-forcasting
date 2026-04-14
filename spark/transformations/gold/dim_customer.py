from itertools import chain

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.common.data_loading import read_postgresql_table
from spark.common.gold_utils import reorder_columns, add_surrogate_key


def prepare_internal_customers(spark: SparkSession) -> DataFrame:
    silver_customer_df = read_postgresql_table(
        spark=spark, schema="silver", table="customers")
    customer_df = silver_customer_df \
        .select("customer_code", "customer_type", "customer_name", "is_professional", "is_active")\
        .withColumnRenamed("customer_code", "BK_dim_customer")\
        .withColumn("customer_status",
                    F.when(F.col("is_active") == F.lit(True), F.lit("active"))
                    .otherwise(F.lit("inactive")))\
        .withColumn("customer_segment",
                    F.when(F.col("is_professional") ==
                           F.lit(True), F.lit("professional"))
                    .otherwise(F.lit("individual")))\
        .dropDuplicates()
    return customer_df


def add_dim_customer_surrogate_key(df: DataFrame) -> DataFrame:
    order_by_list = ["BK_dim_customer", "customer_name"]
    cleaned_df = add_surrogate_key(
        df=df, surrogate_key="PK_dim_customer", ordered_list=order_by_list)
    return cleaned_df


def reorder_dim_customer_columns(df: DataFrame) -> DataFrame:
    ordered_list = ["PK_dim_customer", "BK_dim_customer", "customer_name",                    
                    "customer_type", "customer_status", "customer_segment"]
    ordered_df = reorder_columns(df=df, ordered_list=ordered_list)
    return ordered_df
