import os
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
import logging

from spark.common.config import BRONZE_PATH
from spark.common.spark_session import build_spark_session
from spark.common.data_loading import read_csv
from spark.common.clean_utils import trim_lower_column, clean_boolean_types, clean_capital_name

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
    cleaned_df = clean_capital_name(df, 'customer_name')\
        .filter(F.col('customer_name').isNotNull())
    return cleaned_df

def clean_customer_type(df:DataFrame) -> DataFrame:
    cleaned_df = trim_lower_column(df, 'customer_type')\
        .withColumn('customer_type', F.ifnull(F.col('customer_type'), F.lit("private")))
    return cleaned_df

if __name__ == "__main__":
    spark = build_spark_session(app_name="data_loading")
    df = read_csv(spark,os.path.join(BRONZE_PATH, "customers.csv"))
    logger.info("Preview of customers.csv")
    df.show(5, truncate=False)
    cleaned_df= clean_customer_type(df)
    logger.info("Preview of formated customers.csv")
    cleaned_df.show(5, truncate=False)