import os
import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.common.spark_session import build_spark_session
from spark.common.data_loading import read_postgresql_table, save_into_db
from spark.common.clean_utils import clean_ids, clean_decimal, clean_cost, standardize_date, standardize_datetime, clean_int, clean_is_active_types, clean_capital_name, trim_lower_column, clean_address, standardize_postal_code

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_purchase_orders() -> DataFrame:
    spark = build_spark_session('clean_purchase_orders')
    df = read_postgresql_table(spark=spark, schema='bronze', table='purchase_orders')
    cleaned_df = clean_ids(df, "supplier_id")
    cleaned_df = clean_ids(cleaned_df, "location_id")
    cleaned_df = clean_ids(cleaned_df, "po_id")
    cleaned_df = standardize_datetime(cleaned_df, "created_at")
    cleaned_df = standardize_date(cleaned_df, "expected_delivery")
    cleaned_df = standardize_date(cleaned_df, "actual_delivery")
    cleaned_df = trim_lower_column(cleaned_df, "status")
    cleaned_df = clean_cost(cleaned_df, "total_cost_eur")

    #save_into_db(schema='silver', table='purchase_orders', dataframe=cleaned_df, mode='append')
    save_into_db(schema='staging', table='purchase_orders', dataframe=cleaned_df)
    return cleaned_df

if __name__ == "__main__":
    clean_purchase_orders()
    
