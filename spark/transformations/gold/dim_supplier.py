from itertools import chain

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.common.data_loading import read_postgresql_table
from spark.common.gold_utils import reorder_columns, add_surrogate_key

def prepare_internal_suppliers(spark: SparkSession) -> DataFrame:
    silver_supplier_df = read_postgresql_table(
        spark=spark, schema="silver", table="suppliers"
    )
    supplier_df = silver_supplier_df \
        .select("supplier_code", "supplier_name", "contact_name", "country", "is_active")\
        .withColumnRenamed("supplier_code", "BK_dim_supplier")\
        .withColumn("supplier_status",
                    F.when(F.col("is_active") == F.lit(True), F.lit("active"))
                    .otherwise(F.lit("inactive")))\
        .dropDuplicates()
    return supplier_df

def add_dim_supplier_surrogate_key(df: DataFrame) -> DataFrame:
    order_by_list = ["BK_dim_supplier", "supplier_name"]
    cleaned_df = add_surrogate_key(
        df=df, surrogate_key="PK_dim_supplier", ordered_list=order_by_list)
    return cleaned_df

def reorder_dim_supplier_columns(df: DataFrame) -> DataFrame:
    ordered_list = ["PK_dim_supplier", "BK_dim_supplier",  "supplier_name", "contact_name", "country", "supplier_status"]
    ordered_df = reorder_columns(df=df, ordered_list=ordered_list)
    return ordered_df
