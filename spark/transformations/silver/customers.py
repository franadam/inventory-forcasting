import os
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
import logging

from spark.common.spark_session import build_spark_session
from spark.common.data_loading import read_csv
from spark.common.config import BRONZE_PATH

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def standardize_customer_code(df:DataFrame) -> DataFrame:
    #CUS0001 -> CUST-D001
    cleaned_df = (df.withColumn('customer_code', 
                        F.regexp_replace(F.col('customer_code'), 
                                        r"000", 'T-D00'))
    )    
    return cleaned_df

if __name__ == "__main__":
    spark = build_spark_session(app_name="data_loading")
    df = read_csv(spark,os.path.join(BRONZE_PATH, "customers.csv"))
    logger.info("Preview of customers.csv")
    df.show(5, truncate=False)
    cleaned_df= standardize_customer_code(df)
    logger.info("Preview of formated customers.csv")
    cleaned_df.show(5, truncate=False)