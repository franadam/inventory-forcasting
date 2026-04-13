import logging
from pyspark.sql import DataFrame

from spark.common.spark_session import build_spark_session
from spark.common.data_loading import read_postgresql_table, save_into_db
from spark.common.clean_utils import clean_decimal, clean_ids, standardize_datetime, trim_lower_column

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_inventory_movements() -> DataFrame:
    spark = build_spark_session('clean_inventory_movements')
    df = read_postgresql_table(spark=spark, schema='bronze', table='inventory_movements')
    cleaned_df = clean_decimal(df, "qty_delta")
    cleaned_df = clean_ids(cleaned_df, "movement_id")
    cleaned_df = clean_ids(cleaned_df, "product_id")
    cleaned_df = clean_ids(cleaned_df, "location_id")
    cleaned_df = clean_ids(cleaned_df, "ref_order_id")
    cleaned_df = trim_lower_column(cleaned_df, "movement_type")
    cleaned_df = trim_lower_column(cleaned_df, "ref_order_type")
    cleaned_df = trim_lower_column(cleaned_df, "notes")
    cleaned_df = standardize_datetime(cleaned_df, "movement_ts")
    
    #save_into_db(schema='silver', table='inventory_movements', dataframe=cleaned_df, mode='append')
    save_into_db(schema='staging', table='inventory_movements', dataframe=cleaned_df)
    return cleaned_df

if __name__ == "__main__":
    clean_inventory_movements()
    
