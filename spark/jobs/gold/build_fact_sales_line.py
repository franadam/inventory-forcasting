import logging

from pyspark.sql import DataFrame

from spark.common.spark_session import build_spark_session
from spark.common.dataframe_utils import save_into_db
from spark.transformations.gold.fact_sales_line import prepare_fact_sales_line, validate_fact_sales_line, reorder_fact_sales_columns

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_fact_sales_line() -> DataFrame:
    spark = build_spark_session("build_fact_sales_line")

    fact_sales_line_df = prepare_fact_sales_line(spark)
    fact_sales_line_df = validate_fact_sales_line(fact_sales_line_df)
    fact_sales_line_df = reorder_fact_sales_columns(fact_sales_line_df)

    save_into_db(schema='gold', table='fact_sales_line',
                 dataframe=fact_sales_line_df, mode="append")

    return fact_sales_line_df


if __name__ == "__main__":
    fact_sales_line_df = build_fact_sales_line()
    fact_sales_line_df.show(truncate=False)
