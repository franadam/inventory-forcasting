from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DecimalType

from spark.common.spark_session import build_spark_session
from spark.common.dataframe_utils import read_postgresql_table, show_null_columns
from spark.common.gold_utils import reorder_columns, add_surrogate_key


def load_silver_tables(spark: SparkSession) -> dict[str, DataFrame]:
    inventory_df = read_postgresql_table(
        spark=spark, schema="silver", table="inventory")

    return inventory_df


def load_gold_dimensions(spark: SparkSession) -> dict[str, DataFrame]:
    dim_date_df = read_postgresql_table(
        spark=spark, schema="gold", table="dim_date")
    dim_product_df = read_postgresql_table(
        spark=spark, schema="gold", table="dim_product")
    dim_location_df = read_postgresql_table(
        spark=spark, schema="gold", table="dim_location")

    return {
        "dim_date": dim_date_df,
        "dim_product": dim_product_df,
        "dim_location": dim_location_df,
    }


def prepare_fact_inventory_snapshot(spark: SparkSession) -> DataFrame:
    gold = load_gold_dimensions(spark)

    iv = load_silver_tables(spark).alias("iv")

    dd = gold["dim_date"].alias("dd")
    dp = gold["dim_product"].alias("dp")
    dl = gold["dim_location"].alias("dl")


    fact_df = (
        iv
        .withColumn("last_updated_date", F.col("last_updated").cast(DateType()))
        .join(
            dp,
            F.col("iv.product_id") == F.col("dp.product_id_source"),
            how="left",
        )
        .join(
            dl,
            F.col("iv.location_id") == F.col("dl.location_id_source"),
            how="left",
        )
        .join(
            dd,
            F.col("last_updated_date") == F.col("dd.full_date"),
            how="left",
        )
        .withColumn("available_stock", F.col("qty_on_hand") - F.col("qty_reserved"))
        .withColumn("is_ruptured", F.col("available_stock") <= F.lit(0))
        .withColumn("is_below_min", F.col("available_stock") < F.col("min_stock_level"))
        .withColumn("is_below_reorder", F.col("available_stock") < F.col("reorder_point"))
        .select(
            F.col("dd.date_key").alias("last_updated_date_key"),
            F.col("dp.SK_dim_product").alias("SK_dim_product"),
            F.col("dl.SK_dim_location").alias("SK_dim_location"),
            F.col("inventory_id").alias("inventory_id_dd"),
            F.col("iv.qty_on_hand").cast(
                DecimalType(15,2)),
            F.col("iv.qty_reserved").cast(
                DecimalType(15,2)),
            F.col("iv.min_stock_level").cast(
                DecimalType(15,2)),
            F.col("iv.max_stock_level").cast(
                DecimalType(15,2)),
            F.col("reorder_point").cast(DecimalType(15,2)),
            F.col("available_stock").cast(DecimalType(15,2)),
            F.col("is_ruptured"),
            F.col("is_below_min"),
            F.col("is_below_reorder")
        )
    )

    return fact_df


def reorder_fact_inventory_snapshot_columns(df: DataFrame) -> DataFrame:
    ordered_list = ["last_updated_date_key", "SK_dim_product", "SK_dim_location", "inventory_id_dd", "qty_on_hand",
                    "qty_reserved", "min_stock_level", "max_stock_level", "reorder_point", "available_stock", "is_ruptured", "is_below_min",
                    "is_below_reorder"]
    ordered_df = reorder_columns(df=df, ordered_list=ordered_list)
    return ordered_df


def validate_fact_inventory_snapshot(df: DataFrame) -> DataFrame:
    """
    Basic validation layer.
    Keeps only rows with mandatory surrogate keys and business keys.
    """
    return df.filter(
        F.col("last_updated_date_key").isNotNull()
        & F.col("SK_dim_product").isNotNull()
        & F.col("SK_dim_location").isNotNull()
        & F.col("inventory_id_dd").isNotNull()
    )


if __name__ == "__main__":
    spark = build_spark_session(app_name="fact_inventory_snapshot")
    df = prepare_fact_inventory_snapshot(spark)
    df = reorder_fact_inventory_columns(df)
    df.show(5)