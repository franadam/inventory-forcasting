import os
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Column
import logging

from spark.common.config import BRONZE_PATH
from spark.common.spark_session import build_spark_session
from spark.common.data_loading import read_csv
from spark.common.clean_utils import trim_lower_column, clean_capital_name

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def location_code_helper(type:Column, city:Column):
    #F.concat_ws("-", F.col("location_type"), F.upper(F.substring(F.col("city"), 0, 3)))
    type_code = F.when(type=="regional warehouse", "WH").otherwise("LOC")
    city_code = F.upper(F.substring(city, 0, 3))
    return F.concat_ws("-", type_code, city_code)

def standardize_location_code(df:DataFrame) -> DataFrame:
    #LOC0001 -> sanicenter-BRU  -> LOC-BRU
    cleaned_df = df \
                    .withColumn("location_code", F.trim(F.col("location_code"))) \
                    .withColumn("location_code", location_code_helper(F.col("location_type"), F.col("city")))
    return cleaned_df

def clean_location_type(df:DataFrame) -> DataFrame:
    patern = r"(?<=[a-zA-Z])[^a-zA-Z]+(?=[a-zA-Z])"
    cleaned_df = trim_lower_column(df, "location_type")\
                    .withColumn("location_type", 
                                F.regexp_replace(F.col("location_type"), patern, " "))
    return cleaned_df
    
def clean_location_name(df:DataFrame) -> DataFrame:
    cleaned_df = clean_capital_name(df, "location_name")
    return cleaned_df
    


if __name__ == "__main__":
    spark = build_spark_session(app_name="data_loading")
    df = read_csv(spark,os.path.join(BRONZE_PATH, "locations.csv"))
    logger.info("Preview of locations.csv")
    df.show()
    cleaned_df= standardize_location_code(df)
    logger.info("Preview of formated locations.csv")
    cleaned_df.show()