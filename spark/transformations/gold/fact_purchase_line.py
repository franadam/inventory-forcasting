from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DecimalType

from spark.common.spark_session import build_spark_session
from spark.common.dataframe_utils import read_postgresql_table, show_null_columns
from spark.common.gold_utils import reorder_columns, add_surrogate_key


def load_silver_tables(spark: SparkSession) -> dict[str, DataFrame]:
    purchase_orders_df = read_postgresql_table(
        spark=spark, schema="silver", table="purchase_orders")
    purchase_order_lines_df = read_postgresql_table(
        spark=spark, schema="silver", table="purchase_order_lines")

    return {
        "purchase_orders": purchase_orders_df,
        "purchase_order_lines": purchase_order_lines_df
    }


def load_gold_dimensions(spark: SparkSession) -> dict[str, DataFrame]:
    dim_date_df = read_postgresql_table(
        spark=spark, schema="gold", table="dim_date")
    dim_product_df = read_postgresql_table(
        spark=spark, schema="gold", table="dim_product")
    dim_supplier_df = read_postgresql_table(
        spark=spark, schema="gold", table="dim_supplier")
    dim_location_df = read_postgresql_table(
        spark=spark, schema="gold", table="dim_location")
    dim_purchase_status_df = read_postgresql_table(
        spark=spark, schema="gold", table="dim_purchase_status")

    return {
        "dim_date": dim_date_df,
        "dim_product": dim_product_df,
        "dim_supplier": dim_supplier_df,
        "dim_location": dim_location_df,
        "dim_purchase_status": dim_purchase_status_df,
    }


def prepare_fact_purchase_line(spark: SparkSession) -> DataFrame:
    silver = load_silver_tables(spark)
    gold = load_gold_dimensions(spark)

    po = silver["purchase_orders"].alias("po")
    pol = silver["purchase_order_lines"].alias("pol")

    dd_created = gold["dim_date"].alias("dd_created")
    dd_expected = gold["dim_date"].alias("dd_expected")
    dd_actual = gold["dim_date"].alias("dd_actual")

    dp = gold["dim_product"].alias("dp")
    ds = gold["dim_supplier"].alias("ds")
    dl = gold["dim_location"].alias("dl")
    dps = gold["dim_purchase_status"].alias("dps")

    fact_df = (
        pol
        .join(po, on="po_id", how="inner")
        .withColumn("created_date", F.to_date(F.col("po.created_at")))
        .join(
            dp,
            F.col("pol.product_id") == F.col("dp.product_id_source"),
            how="left",
        )
        .join(
            ds,
            F.col("po.supplier_id") == F.col("ds.supplier_id_source"),
            how="left",
        )
        .join(
            dl,
            F.col("po.location_id") == F.col("dl.location_id_source"),
            how="left",
        )
        .join(
            dps,
            F.col("po.status") == F.col("dps.status_name"),
            how="left",
        )
        .join(
            dd_created,
            F.col("created_date") == F.col("dd_created.full_date"),
            how="left",
        )
        .join(
            dd_expected,
            F.col("po.expected_delivery") == F.col("dd_expected.full_date"),
            how="left",
        )
        .join(
            dd_actual,
            F.col("po.actual_delivery") == F.col("dd_actual.full_date"),
            how="left",
        )
        .select(
            F.col("pol.line_id").alias("purchase_order_line_id_dd"),
            F.col("po.po_id").alias("purchase_order_id_dd"),
            F.col("dd_created.date_key").alias("created_date_key"),
            F.col("dd_expected.date_key").alias("expected_date_key"),
            F.col("dd_actual.date_key").alias("actual_date_key"),
            F.col("dp.SK_dim_product").alias("SK_dim_product"),
            F.col("dl.SK_dim_location").alias("SK_dim_location"),
            F.col("ds.SK_dim_supplier").alias("SK_dim_supplier"),
            F.col("dps.SK_dim_purchase_status").alias(
                "SK_dim_purchase_status"),
            F.col("pol.qty_ordered").cast(
                DecimalType(15, 2)).alias("qty_ordered"),
            F.col("pol.qty_received").cast(
                DecimalType(15, 2)).alias("qty_received"),
            F.col("pol.unit_cost_eur").cast(
                DecimalType(15, 2)).alias("unit_cost_eur"),
            F.col("pol.line_total_eur").cast(
                DecimalType(15, 2)).alias("gross_purchase_amount")
        )
    )

    return fact_df


def reorder_fact_purchase_columns(df: DataFrame) -> DataFrame:
    ordered_list = ["created_date_key", "expected_date_key", "actual_date_key",
                    "SK_dim_product", "SK_dim_location", "SK_dim_supplier",
                    "SK_dim_purchase_status", "purchase_order_line_id_dd",
                    "purchase_order_id_dd", "qty_ordered", "qty_received",
                    "unit_cost_eur", "gross_purchase_amount"]
    ordered_df = reorder_columns(df=df, ordered_list=ordered_list)
    return ordered_df


def validate_fact_purchase_line(df: DataFrame) -> DataFrame:
    """
    Basic validation layer.
    Keeps only rows with mandatory surrogate keys and business keys.
    """
    return df.filter(
        F.col("created_date_key").isNotNull()
        & F.col("expected_date_key").isNotNull()
        & F.col("SK_dim_product").isNotNull()
        & F.col("SK_dim_location").isNotNull()
        & F.col("SK_dim_supplier").isNotNull()
        & F.col("purchase_order_id_dd").isNotNull()
    )


if __name__ == "__main__":
    spark = build_spark_session(app_name="fact_purchase_line")
    df = prepare_fact_purchase_line(spark)
    df = reorder_fact_purchase_columns(df)
    print("\ndf columns:", df.columns, "\n")
    show_null_columns(df)
    df.show(5)
