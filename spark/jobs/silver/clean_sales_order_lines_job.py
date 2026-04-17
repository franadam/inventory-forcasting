import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.common.spark_session import build_spark_session
from spark.common.dataframe_utils import read_postgresql_table, save_into_db
from spark.common.clean_utils import clean_stock, clean_ids, clean_cost

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def clean_sales_order_lines() -> DataFrame:
    spark = build_spark_session('clean_sales_order_lines')
    df = read_postgresql_table(
        spark=spark, schema='bronze', table='sales_order_lines')
    cleaned_df = clean_ids(df, "order_id")
    cleaned_df = clean_ids(cleaned_df, "product_id")
    cleaned_df = clean_stock(cleaned_df, "qty_ordered", threshold=0)
    cleaned_df = clean_stock(cleaned_df, "qty_fulfilled", threshold=0)
    cleaned_df = clean_cost(cleaned_df, "unit_price_eur")
    cleaned_df = clean_cost(cleaned_df, "line_total_eur")

    # save_into_db(schema='silver', table='sales_order_lines', dataframe=cleaned_df, mode='append')
    save_into_db(schema='staging', table='sales_order_lines',
                 dataframe=cleaned_df)
    return cleaned_df


if __name__ == "__main__":
    clean_sales_order_lines()
