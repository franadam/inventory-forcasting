import os
import logging
from pyspark.sql import DataFrame

from spark.common.spark_session import build_spark_session
from spark.common.data_loading import read_postgresql_table, save_into_db
from spark.common.clean_utils import clean_decimal, clean_int, clean_is_active_types, clean_capital_name, trim_lower_column, clean_address, standardize_postal_code
from spark.transformations.silver.products import clean_product_name, clean_unit_weight_kg, clean_unit_price_eur,clean_min_order_qty, clean_lead_time_days, standardize_product_code, clean_subcategory

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_products() -> DataFrame:
    spark = build_spark_session('clean_products')
    df = read_postgresql_table(spark=spark, schema='bronze', table='products')
    cleaned_df = standardize_product_code(df)
    cleaned_df = clean_capital_name(cleaned_df, "category")
    cleaned_df = clean_subcategory(cleaned_df)
    cleaned_df = clean_capital_name(cleaned_df, "brand")
    cleaned_df = clean_product_name(cleaned_df)
    cleaned_df = trim_lower_column(cleaned_df, "unit_of_measure")
    cleaned_df = clean_unit_price_eur(cleaned_df)
    cleaned_df = clean_unit_weight_kg(cleaned_df)
    cleaned_df = clean_min_order_qty(cleaned_df)
    cleaned_df = clean_lead_time_days(cleaned_df)
    cleaned_df = clean_is_active_types(cleaned_df)
    
    #save_into_db(schema='silver', table='products', dataframe=cleaned_df, mode='append')
    save_into_db(schema='staging', table='products', dataframe=cleaned_df)
    return cleaned_df

if __name__ == "__main__":
    spark = build_spark_session('clean_products')
    df = read_postgresql_table(spark=spark, schema='bronze', table='products')
    logger.info("Preview of products.csv")
    df.show(5, truncate=False)
    cleaned_df=clean_products()
    logger.info("Preview of formated products.csv") 
    cleaned_df.show(5, truncate=False)
    
