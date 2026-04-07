import os
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
import logging

from spark.common.config import BRONZE_PATH
from spark.common.spark_session import build_spark_session
from spark.common.data_loading import read_csv
from spark.common.clean_utils import trim_lower_column, clean_boolean_types, standardize_postal_code, clean_capital_name

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def standardize_supplier_code(df:DataFrame) -> DataFrame:
    #SUP00001 -> SUP-00001
    patern = r"SUP"
    cleaned_df = df.withColumn("supplier_code", F.trim(F.col("supplier_code"))) \
                    .withColumn("supplier_code", F.regexp_replace(F.col("supplier_code"), patern, "SUP-"))
    return cleaned_df

def clean_supplier_email(df:DataFrame) -> DataFrame:
    patern = r" at "
    cleaned_df = trim_lower_column(df, 'email')\
                    .withColumn("email", F.regexp_replace(F.col("email"), patern, "@"))
    
    return cleaned_df

if __name__ == "__main__":
    spark = build_spark_session(app_name="data_loading")
    df = read_csv(spark,os.path.join(BRONZE_PATH, "suppliers.csv"))
    logger.info("Preview of suppliers.csv")
    df.show(5, truncate=False)
    cleaned_df= standardize_supplier_code(df)
    logger.info("Preview of formated suppliers.csv")
    cleaned_df.show(5, truncate=False)