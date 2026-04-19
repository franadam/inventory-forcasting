from itertools import chain

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.common.dataframe_utils import read_postgresql_table
from spark.common.gold_utils import reorder_columns


def prepare_internal_suppliers(spark: SparkSession) -> DataFrame:
    silver_supplier_df = read_postgresql_table(
        spark=spark, schema="silver", table="suppliers"
    )
    supplier_df = silver_supplier_df \
        .select("supplier_id", "supplier_code", "supplier_name", "contact_name", "country", "email", "avg_lead_time_days", "reliability_score", "is_active")\
        .withColumnRenamed("supplier_id", "supplier_id_source")\
        .withColumn("supplier_status",
                    F.when(F.col("is_active") == F.lit(True), F.lit("active"))
                    .otherwise(F.lit("inactive")))\
        .dropDuplicates()
    return supplier_df


def reorder_dim_supplier_columns(df: DataFrame) -> DataFrame:
    ordered_list = ["supplier_id_source",  "supplier_code", "supplier_name",
                    "contact_name", "supplier_status", "country", "email", "avg_lead_time_days", "reliability_score"]
    ordered_df = reorder_columns(df=df, ordered_list=ordered_list)
    return ordered_df
