import logging
from pyspark.sql import DataFrame

from spark.common.spark_session import build_spark_session
from spark.common.data_loading import read_postgresql_table, save_into_db
from spark.common.clean_utils import clean_is_active_types, clean_city, standardize_postal_code, clean_region, clean_address
from spark.transformations.silver.customers import clean_customer_name, clean_customer_type, standardize_customer_code

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_customers() -> DataFrame:
    spark = build_spark_session('clean_customers')
    df = read_postgresql_table(spark=spark, schema='bronze', table='customers')
    cleaned_df = standardize_customer_code(df)
    cleaned_df = clean_customer_name(cleaned_df)
    cleaned_df = clean_customer_type(cleaned_df)
    cleaned_df = clean_address(cleaned_df)
    cleaned_df = clean_city(cleaned_df)
    cleaned_df = standardize_postal_code(cleaned_df)
    cleaned_df = clean_region(cleaned_df)
    cleaned_df = clean_is_active_types(cleaned_df, column='is_professional')
    cleaned_df = clean_is_active_types(cleaned_df)
    
    #save_into_db(schema='silver', table='customers', dataframe=cleaned_df, mode='append')
    save_into_db(schema='staging', table='customers', dataframe=cleaned_df)
    return cleaned_df  

if __name__ == "__main__":
    clean_customers()