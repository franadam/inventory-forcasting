import logging
from pyspark.sql import DataFrame

from spark.common.spark_session import build_spark_session
from spark.common.data_loading import read_postgresql_table, save_into_db
from spark.common.clean_utils import clean_ids, clean_stock, standardize_datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_inventory() -> DataFrame:
    spark = build_spark_session('clean_inventory')
    df = read_postgresql_table(spark=spark, schema='bronze', table='inventory')
    cleaned_df = clean_ids(df, "product_id")
    cleaned_df = clean_ids(cleaned_df, "location_id")
    cleaned_df = clean_stock(cleaned_df, "qty_on_hand")
    cleaned_df = clean_stock(cleaned_df, "qty_reserved", threshold=0)
    cleaned_df = clean_stock(cleaned_df, "min_stock_level")
    cleaned_df = clean_stock(cleaned_df, "reorder_point", threshold=20)
    cleaned_df = clean_stock(cleaned_df, "max_stock_level", threshold=100)
    cleaned_df = standardize_datetime(cleaned_df, "last_updated")
    
    #save_into_db(schema='silver', table='inventory', dataframe=cleaned_df, mode='append')
    save_into_db(schema='staging', table='inventory', dataframe=cleaned_df)
    return cleaned_df

if __name__ == "__main__":
    clean_inventory()
    
