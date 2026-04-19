from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.common.spark_session import build_spark_session
from spark.common.dataframe_utils import read_postgresql_table
from spark.common.gold_utils import reorder_columns, add_surrogate_key


def load_silver_tables(spark: SparkSession) -> dict[str, DataFrame]:
    sales_orders_df = read_postgresql_table(
        spark=spark, schema="silver", table="sales_orders")
    sales_order_lines_df = read_postgresql_table(
        spark=spark, schema="silver", table="sales_order_lines")

    return {
        "sales_orders": sales_orders_df,
        "sales_order_lines": sales_order_lines_df
    }


def load_gold_dimensions(spark: SparkSession) -> dict[str, DataFrame]:
    dim_date_df = read_postgresql_table(
        spark=spark, schema="gold", table="dim_date")
    dim_product_df = read_postgresql_table(
        spark=spark, schema="gold", table="dim_product")
    dim_customer_df = read_postgresql_table(
        spark=spark, schema="gold", table="dim_customer")
    dim_location_df = read_postgresql_table(
        spark=spark, schema="gold", table="dim_location")
    dim_sales_channel_df = read_postgresql_table(
        spark=spark, schema="gold", table="dim_sales_channel")
    dim_sales_status_df = read_postgresql_table(
        spark=spark, schema="gold", table="dim_sales_status")

    return {
        "dim_date": dim_date_df,
        "dim_product": dim_product_df,
        "dim_customer": dim_customer_df,
        "dim_location": dim_location_df,
        "dim_sales_channel": dim_sales_channel_df,
        "dim_sales_status": dim_sales_status_df,
    }


def prepare_fact_sales_line(spark: SparkSession) -> DataFrame:
    silver = load_silver_tables(spark)
    gold = load_gold_dimensions(spark)

    so = silver["sales_orders"].alias("so")
    sol = silver["sales_order_lines"].alias("sol")

    dd = gold["dim_date"].alias("dd")
    dp = gold["dim_product"].alias("dp")
    dc = gold["dim_customer"].alias("dc")
    dl = gold["dim_location"].alias("dl")
    dsc = gold["dim_sales_channel"].alias("dsc")
    dss = gold["dim_sales_status"].alias("dss")

    fact_df = (
        sol
        .join(so, on="order_id", how="inner")
        .withColumn("order_date", F.to_date(F.col("so.order_ts")))
        .join(
            dp,
            F.col("sol.product_id") == F.col("dp.product_id_source"),
            how="left",
        )
        .join(
            dc,
            F.col("so.customer_id") == F.col("dc.customer_id_source"),
            how="left",
        )
        .join(
            dl,
            F.col("so.location_id") == F.col("dl.location_id_source"),
            how="left",
        )
        .join(
            dsc,
            F.col("so.source") == F.col("dsc.source_name"),
            how="left",
        )
        .join(
            dss,
            F.col("so.status") == F.col("dss.status_name"),
            how="left",
        )
        .join(
            dd,
            F.col("order_date") == F.col("dd.full_date"),
            how="left",
        )
        .select(
            F.col("sol.line_id").alias("sales_order_line_id_dd"),
            F.col("so.order_id").alias("sales_order_id_dd"),
            F.col("dd.date_key").alias("date_key"),
            F.col("dp.SK_dim_product").alias("SK_dim_product"),
            F.col("dl.SK_dim_location").alias("SK_dim_location"),
            F.col("dc.SK_dim_customer").alias("SK_dim_customer"),
            F.col("dsc.SK_dim_sales_channel").alias("SK_dim_sales_channel"),
            F.col("dss.SK_dim_sales_status").alias("SK_dim_sales_status"),
            F.col("sol.qty_ordered").cast(
                "decimal(15,2)").alias("qty_ordered"),
            F.col("sol.qty_fulfilled").cast(
                "decimal(15,2)").alias("qty_fulfilled"),
            F.col("sol.unit_price_eur").cast(
                "decimal(15,2)").alias("unit_price_eur"),
            F.col("sol.line_total_eur").cast(
                "decimal(15,2)").alias("line_sales_amount_eur")
        )
    )

    return fact_df


def reorder_fact_sales_columns(df: DataFrame) -> DataFrame:
    ordered_list = ["date_key", "SK_dim_product", "SK_dim_location", "SK_dim_customer", "SK_dim_sales_channel",
                    "SK_dim_sales_status", "sales_order_line_id_dd", "sales_order_id_dd", "qty_ordered", "qty_fulfilled", "unit_price_eur", "line_sales_amount_eur"]
    ordered_df = reorder_columns(df=df, ordered_list=ordered_list)
    return ordered_df


def validate_fact_sales_line(df: DataFrame) -> DataFrame:
    """
    Basic validation layer.
    Keeps only rows with mandatory surrogate keys and business keys.
    """
    return df.filter(
        F.col("date_key").isNotNull()
        & F.col("SK_dim_product").isNotNull()
        & F.col("SK_dim_location").isNotNull()
        & F.col("SK_dim_customer").isNotNull()
        & F.col("sales_order_line_id_dd").isNotNull()
        & F.col("sales_order_id_dd").isNotNull()
    )


if __name__ == "__main__":
    spark = build_spark_session(app_name="fact_sales_line")
    df = prepare_fact_sales_line(spark)
    # df = get_foreign_keys2(spark, fact_table="sales_order_lines", dim_table="dim_customer", join_keys=[
    #                       "customer_id", "customer_id_source"], fact_id_column="line_id")
    df = add_fact_sales_surrogate_key(df)
    df = reorder_fact_sales_columns(df)
    df.show()
