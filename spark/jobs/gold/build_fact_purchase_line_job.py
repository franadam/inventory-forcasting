import logging

from pyspark.sql import DataFrame

from spark.common.spark_session import build_spark_session
from spark.common.dataframe_utils import save_into_db
from spark.transformations.gold.fact_purchase_line import prepare_fact_purchase_line, validate_fact_purchase_line, reorder_fact_purchase_columns

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_fact_purchase_line() -> DataFrame:
    spark = build_spark_session("build_fact_purchase_line")

    fact_purchase_line_df = prepare_fact_purchase_line(spark)
    fact_purchase_line_df = validate_fact_purchase_line(fact_purchase_line_df)
    fact_purchase_line_df = reorder_fact_purchase_columns(
        fact_purchase_line_df)

    save_into_db(schema='gold', table='fact_purchase_line',
                 dataframe=fact_purchase_line_df, mode="append")

    return fact_purchase_line_df


if __name__ == "__main__":
    df = build_fact_purchase_line()
    df.show()
