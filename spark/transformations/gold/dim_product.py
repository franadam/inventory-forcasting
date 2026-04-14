from itertools import chain

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.common.data_loading import read_postgresql_table
from spark.common.gold_utils import reorder_columns, add_surrogate_key

def prepare_internal_products(spark: SparkSession) -> DataFrame:
    silver_product_df = read_postgresql_table(
        spark=spark, schema="silver", table="products")
    product_df = silver_product_df \
        .select("sku", "category", "subcategory", "product_name", "brand", "unit_of_measure", "is_active")\
        .withColumnRenamed("sku", "BK_dim_product")\
        .withColumn("product_status",
                    F.when(F.col("is_active") == F.lit(True), F.lit("active"))
                    .otherwise(F.lit("inactive")))\
        .dropDuplicates()
    return product_df

def add_dim_product_surrogate_key(df: DataFrame) -> DataFrame:
    order_by_list = ["BK_dim_product", "product_name"]
    cleaned_df = add_surrogate_key(
        df=df, surrogate_key="PK_dim_product", ordered_list=order_by_list)
    return cleaned_df


def reorder_dim_product_columns(df: DataFrame) -> DataFrame:
    ordered_list = ["PK_dim_product", "BK_dim_product", "product_name", "category", "subcategory", "product_status", "brand", "unit_of_measure"]
    ordered_df = reorder_columns(df=df, ordered_list=ordered_list)
    return ordered_df
