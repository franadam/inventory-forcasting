from itertools import chain

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.common.dataframe_utils import read_postgresql_table
from spark.common.gold_utils import reorder_columns


def prepare_internal_products(spark: SparkSession) -> DataFrame:
    silver_product_df = read_postgresql_table(
        spark=spark, schema="silver", table="products")
    product_df = silver_product_df \
        .select("product_id", "sku", "category", "subcategory", "product_name", "brand", "unit_of_measure", "unit_weight_kg", "lead_time_days", "min_order_qty", "is_active")\
        .withColumnRenamed("product_id", "product_id_source")\
        .withColumn("product_status",
                    F.when(F.col("is_active") == F.lit(True), F.lit("active"))
                    .otherwise(F.lit("inactive")))\
        .dropDuplicates()
    return product_df


def reorder_dim_product_columns(df: DataFrame) -> DataFrame:
    ordered_list = ["product_id_source", "sku", "product_name",  "category",
                    "subcategory", "brand", "unit_of_measure", "unit_weight_kg", "lead_time_days", "min_order_qty"]
    ordered_df = reorder_columns(df=df, ordered_list=ordered_list)
    return ordered_df
