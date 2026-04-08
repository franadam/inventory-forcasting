import os
import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.common.spark_session import build_spark_session
from spark.common.data_loading import read_postgresql_table, save_into_db
from spark.common.clean_utils import clean_decimal, standardize_date, clean_int, clean_is_active_types, clean_capital_name, trim_lower_column, clean_address, standardize_postal_code

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_inventory() -> DataFrame:
    spark = build_spark_session('clean_inventory')
    df = read_postgresql_table(spark=spark, schema='bronze', table='inventory')
    cleaned_df = clean_decimal(df, "qty_on_hand")
    cleaned_df = clean_decimal(cleaned_df, "qty_reserved")
    cleaned_df = clean_decimal(cleaned_df, "min_stock_level")
    cleaned_df = clean_decimal(cleaned_df, "reorder_point")
    cleaned_df = clean_decimal(cleaned_df, "max_stock_level")
    cleaned_df = standardize_date(cleaned_df, "last_updated")
    
    #save_into_db(schema='silver', table='inventory', dataframe=cleaned_df, mode='append')
    save_into_db(schema='staging', table='inventory', dataframe=cleaned_df)
    return cleaned_df

if __name__ == "__main__":
    spark = build_spark_session('clean_inventory')
    df = read_postgresql_table(spark=spark, schema='bronze', table='inventory')
    logger.info("Preview of inventory.csv")
    df.show(5, truncate=False)
    last_updated = df.last_updated
    last_updated_array = df.select('last_updated').toPandas()['last_updated']
    cleaned_df= clean_inventory()
    #print(last_updated_array)
    logger.info("Preview of formated inventory.csv")
    cleaned_df.show(5, truncate=False)
    #clean_inventory()
    
