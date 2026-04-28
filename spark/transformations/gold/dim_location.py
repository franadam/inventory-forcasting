from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.common.dataframe_utils import read_postgresql_table
from spark.common.gold_utils import build_province_region_map, reorder_columns, enrich_location_geography


def prepare_internal_locations(spark: SparkSession) -> DataFrame:
    province_region_map = build_province_region_map()
    silver_location_df = read_postgresql_table(
        spark=spark, schema="silver", table="locations")
    location_df = enrich_location_geography(silver_location_df, province_region_map) \
        .select("location_id", "location_code", "location_name", "location_type", "is_active", "storage_capacity_m3", "address", "city", "postal_code", "province", "region")\
        .withColumnRenamed("location_id", "location_id_source")\
        .withColumn("location_status",
                    F.when(F.col("is_active") == F.lit(True), F.lit("active"))
                    .otherwise(F.lit("inactive")))\
        .dropDuplicates()
    return location_df


def reorder_dim_location_columns(df: DataFrame) -> DataFrame:
    ordered_list = ["location_id_source", "location_code", "location_name", "location_type", "location_status", "storage_capacity_m3",
                    "address", "city", "postal_code", "province", "region"]
    ordered_df = reorder_columns(df=df, ordered_list=ordered_list)
    return ordered_df
