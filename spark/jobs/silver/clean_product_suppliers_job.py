import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.common.spark_session import build_spark_session
from spark.common.dataframe_utils import read_postgresql_table, save_into_db
from spark.common.clean_utils import clean_cost, clean_lead_time_days, clean_ids, clean_is_active_types

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def clean_product_suppliers() -> DataFrame:
    spark = build_spark_session('clean_product_suppliers')
    df = read_postgresql_table(
        spark=spark, schema='bronze', table='product_suppliers')
    cleaned_df = clean_cost(df, "supplier_unit_cost_eur")
    cleaned_df = clean_ids(cleaned_df, "product_id")
    cleaned_df = clean_ids(cleaned_df, "supplier_id")
    cleaned_df = clean_is_active_types(cleaned_df, "is_preferred")
    cleaned_df = clean_lead_time_days(cleaned_df, "supplier_lead_time_days")

    # save_into_db(schema='silver', table='product_suppliers', dataframe=cleaned_df, mode='append')
    save_into_db(schema='staging', table='product_suppliers',
                 dataframe=cleaned_df)
    return cleaned_df


if __name__ == "__main__":
    clean_product_suppliers()
