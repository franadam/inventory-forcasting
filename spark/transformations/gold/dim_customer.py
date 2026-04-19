from itertools import chain

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.common.dataframe_utils import read_postgresql_table
from spark.common.gold_utils import enrich_location_geography, build_province_region_map, reorder_columns


def prepare_internal_customers(spark: SparkSession) -> DataFrame:
    province_region_map = build_province_region_map()
    silver_customer_df = read_postgresql_table(
        spark=spark, schema="silver", table="customers")
    customer_df = enrich_location_geography(silver_customer_df, province_region_map)\
        .select("customer_id", "customer_code", "customer_type", "customer_name", "is_professional", "is_active", "address", "city", "postal_code", "province", "region")\
        .withColumnRenamed("customer_id", "customer_id_source")\
        .withColumn("customer_status",
                    F.when(F.col("is_active") == F.lit(True), F.lit("active"))
                    .otherwise(F.lit("inactive")))\
        .withColumn("customer_segment",
                    F.when(F.col("is_professional") ==
                           F.lit(True), F.lit("professional"))
                    .otherwise(F.lit("individual")))\
        .dropDuplicates()
    return customer_df


def reorder_dim_customer_columns(df: DataFrame) -> DataFrame:
    ordered_list = ["customer_id_source", "customer_code", "customer_name",
                    "customer_type", "customer_status", "customer_segment", "address", "city", "postal_code", "province", "region"]
    ordered_df = reorder_columns(df=df, ordered_list=ordered_list)
    return ordered_df
