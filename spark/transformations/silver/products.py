import os
import logging
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from spark.common.config import BRONZE_PATH
from spark.common.spark_session import build_spark_session
from spark.common.data_loading import read_csv
from spark.common.clean_utils import clean_decimal, clean_int, clean_capital_name

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def standardize_product_code(df:DataFrame) -> DataFrame:
    #SKU00001 -> SKU-00001
    patern = r"SKU"
    cleaned_df = df.withColumn("sku", F.trim(F.col("sku"))) \
                    .withColumn("sku", F.regexp_replace(F.col("sku"), patern, "SKU-"))
    return cleaned_df

def clean_subcategory(df:DataFrame) -> DataFrame:
    cleaned_df = clean_capital_name(df, "subcategory")
    cleaned_df = cleaned_df.fillna(value = "Unknown", subset = ["subcategory"])
    return cleaned_df
    
def clean_lead_time_days(df:DataFrame) -> DataFrame:
    patern = r"days"
    cleaned_df = df.withColumn("lead_time_days", F.trim(F.col("lead_time_days"))) \
                    .withColumn("lead_time_days", F.regexp_replace(F.col("lead_time_days"), patern, ""))
    #cleaned_df = clean_decimal(cleaned_df, "lead_time_days")\
    #                .withColumn("lead_time_days", F.ceiling(F.col("lead_time_days")))
    cleaned_df = clean_int(cleaned_df, "lead_time_days")\
                    .where(F.col("lead_time_days") > 0)
    return cleaned_df
    
def clean_min_order_qty(df:DataFrame) -> DataFrame:
    cleaned_df = df.withColumn("min_order_qty", F.trim(F.col("min_order_qty"))) 
    qty = F.ceiling(F.col("min_order_qty"))
    cleaned_df = clean_decimal(cleaned_df, "min_order_qty")\
                    .withColumn("min_order_qty",         
                    F.when(qty > 0, qty)\
                    .otherwise(1)
    )
    cleaned_df = clean_int(cleaned_df, "min_order_qty")
    return cleaned_df

def clean_unit_price_eur(df:DataFrame) -> DataFrame:
    cleaned_df = df.withColumn("unit_price_eur", F.regexp_replace(F.col("unit_price_eur"), "-", ""))
    #                .where(F.col("unit_price_eur") > 0)
    cleaned_df = clean_decimal(cleaned_df, "unit_price_eur")
    return cleaned_df

def clean_unit_weight_kg(df:DataFrame) -> DataFrame:
    cleaned_df = df.withColumn("unit_weight_kg", F.regexp_replace(F.col("unit_weight_kg"), "-", ""))
    #                .where(F.col("unit_weight_kg") > 0)
    cleaned_df = clean_decimal(cleaned_df, "unit_weight_kg")
    return cleaned_df

def clean_product_name(df:DataFrame) -> DataFrame:   
    cleaned_df = df.withColumn("product_name", F.trim(F.col("product_name")))
    return cleaned_df

if __name__ == "__main__":
    spark = build_spark_session(app_name="data_loading")
    df = read_csv(spark,os.path.join(BRONZE_PATH, "products.csv"))
    logger.info("Preview of products.csv")
    df.show(5, truncate=False)
    cleaned_df= standardize_product_code(df)
    logger.info("Preview of formated products.csv")
    cleaned_df.show(5, truncate=False)