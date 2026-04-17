from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.common.spark_session import build_spark_session
from spark.common.dataframe_utils import save_into_db


def build_date_dimension(
    spark: SparkSession,
    start_date: str = "2025-01-01",
    end_date: str = "2030-12-31",
) -> DataFrame:

    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)

    if start > end:
        raise ValueError("start_date must be less than or equal to end_date")

    day_count = (end - start).days + 1

    dim_date = (
        spark.range(day_count)
        .select(
            F.date_add(F.lit(start_date), F.col(
                "id").cast("int")).alias("full_date")
        )
        .select(
            F.date_format("full_date", "yyyyMMdd").cast(
                "int").alias("date_key"),
            F.col("full_date"),
            F.year("full_date").alias("year"),
            F.quarter("full_date").alias("quarter"),
            F.month("full_date").alias("month"),
            F.date_format("full_date", "MMMM").alias("month_name"),
            F.weekofyear("full_date").alias("week_of_year"),
            F.dayofyear("full_date").alias("day_of_year"),
            F.dayofmonth("full_date").alias("day_of_month"),
            F.dayofweek("full_date").alias("day_of_week"),
            F.date_format("full_date", "EEEE").alias("day_name"),
            F.when(F.dayofweek("full_date").isin(1, 7), F.lit(True))
             .otherwise(F.lit(False))
             .alias("is_weekend"),
        )
    )
    save_into_db(schema='gold', table='dim_date',
                 dataframe=dim_date)

    return dim_date


if __name__ == "__main__":
    spark = build_spark_session("build_date_dimension")
    df = build_date_dimension(spark)
