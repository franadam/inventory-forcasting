import os
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
import logging

from spark.common.config import BRONZE_PATH
from spark.common.spark_session import build_spark_session
from spark.common.data_loading import read_csv
from spark.common.clean_utils import trim_lower_column, clean_boolean_types

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def standardize_customer_code(df:DataFrame) -> DataFrame:
    #CUS0001 -> CUST-D001
    cleaned_df = (df
                    .withColumn('customer_code',
                        F.trim(F.col('customer_code')))
                    .withColumn('customer_code', 
                        F.regexp_replace(F.col('customer_code'), 
                                        r"000", 'T-D00'))
    )    
    return cleaned_df

def clean_customer_name(df:DataFrame) -> DataFrame:
    cleaned_df = trim_lower_column(df, 'customer_name')\
                    .withColumn('customer_name', F.initcap(F.col('customer_name')))
    return cleaned_df

def clean_customer_type(df:DataFrame) -> DataFrame:
    cleaned_df = trim_lower_column(df, 'customer_type')
    return cleaned_df

def clean_address(df:DataFrame) -> DataFrame:
    cleaned_df = trim_lower_column(df, 'address')\
                    .withColumn('address', F.initcap(F.col('address')))
    return cleaned_df

def clean_city(df:DataFrame) -> DataFrame:
    cleaned_df = trim_lower_column(df, 'city')\
                    .withColumn('city',
                        F.initcap(F.col('city'))
    )
    return cleaned_df

def standardize_postal_code(df:DataFrame) -> DataFrame:
    cleaned_df = df.withColumn('postal_code',
                        F.regexp_extract(F.col('postal_code'),
                                        r"(\d+)", 1)
    )
    return cleaned_df

def clean_region(df:DataFrame) -> DataFrame:
    cleaned_df = trim_lower_column(df, 'region')\
        .withColumn('region', 
                    F.regexp_replace(F.col('region'), r"be-", "")
    )
    return cleaned_df

def clean_is_professional_types(df:DataFrame) -> DataFrame:
    cleaned_df = trim_lower_column(df, 'is_professional')
    cleaned_df = clean_boolean_types(cleaned_df, column='is_professional')
    return cleaned_df

def clean_is_active_types(df:DataFrame) -> DataFrame:
    cleaned_df = trim_lower_column(df, 'is_active')
    cleaned_df = clean_boolean_types(cleaned_df, column='is_active')
    return cleaned_df

if __name__ == "__main__":
    spark = build_spark_session(app_name="data_loading")
    df = read_csv(spark,os.path.join(BRONZE_PATH, "customers.csv"))
    logger.info("Preview of customers.csv")
    df.show(5, truncate=False)
    cleaned_df= clean_region(df)
    logger.info("Preview of formated customers.csv")
    cleaned_df.show(5, truncate=False)