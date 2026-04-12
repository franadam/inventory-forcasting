import os
import re
from word2number import w2n
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import BooleanType, IntegerType, FloatType
from pyspark.sql.functions import udf

def trim_lower_column(df:DataFrame, column:str) -> DataFrame:
    cleaned_df = (df
                    .withColumn(column,
                        F.trim(F.col(column)))
                    .withColumn(column, F.lower(column))
    )
    return cleaned_df

def clean_boolean_types(df:DataFrame, column:str) -> DataFrame:
    cleaned_df = df \
        .withColumn(column,
                        F.initcap(F.col(column)))\
        .withColumn(column, F.lit(F.col(column)).cast(BooleanType()))
    return cleaned_df

def clean_capital_name(df:DataFrame, column:str) -> DataFrame:
    cleaned_df = trim_lower_column(df, column)\
                    .withColumn(column, F.initcap(F.col(column)))
    return cleaned_df

def standardize_postal_code(df:DataFrame, column:str="postal_code") -> DataFrame:
    cleaned_df = df.\
        withColumn(column, F.regexp_extract(F.col(column), r"(\d+)", 1))
    return cleaned_df

def clean_is_active_types(df:DataFrame, column:str="is_active") -> DataFrame:
    cleaned_df = trim_lower_column(df, column)
    cleaned_df = clean_boolean_types(cleaned_df, column)\
        .withColumn(column, F.ifnull(F.col(column), F.lit(False)))
    return cleaned_df

def clean_city(df:DataFrame) -> DataFrame:
    cleaned_df = clean_capital_name(df, 'city')
    return cleaned_df

def clean_region(df:DataFrame) -> DataFrame:
    cleaned_df = trim_lower_column(df, 'region')\
        .withColumn('region', 
                    F.regexp_replace(F.col('region'), r"be-", "")
    )
    return cleaned_df

def clean_address(df:DataFrame) -> DataFrame:
    cleaned_df = clean_capital_name(df, 'address')
    return cleaned_df

def clean_address(df:DataFrame) -> DataFrame:
    cleaned_df = clean_capital_name(df, "address")
    return cleaned_df

def clean_region(df:DataFrame) -> DataFrame:
    cleaned_df = clean_capital_name(df, "region")
    return cleaned_df

def clean_postal_code(df:DataFrame) -> DataFrame:
    cleaned_df = standardize_postal_code(df)
    return cleaned_df

def remove_na(df: DataFrame, column: str) -> DataFrame:
    pattern_na = r"(?i)^\s*n[\s\./-]?a\s*$"
    cleaned_df = df \
        .withColumn(column, F.trim(F.col(column))) \
        .withColumn(
            column,
            F.when(F.col(column).rlike(pattern_na), None)
             .otherwise(F.col(column))
    )
    return cleaned_df

def clean_decimal(df: DataFrame, column: str) -> DataFrame:
    cleaned_df = remove_na(df, column) \
        .withColumn(column, F.regexp_replace(F.col(column), ",", ".")) \
        .withColumn(column, F.col(column).cast(FloatType()))
    return cleaned_df

def clean_cost(df: DataFrame, column: str) -> DataFrame:
    cleaned_df = clean_decimal(df, column)\
        .where(F.col(column) > 0)
    return cleaned_df

def convert_word_number(word):
    try:
        return w2n.word_to_num(word)
    except:
        return None

def clean_int(df:DataFrame, column:str) -> DataFrame:
    convert_udf = udf(convert_word_number, IntegerType())
    cleaned_df = remove_na(df, column) \
        .withColumn(column, F.regexp_replace(F.col(column), r",", ".")) \
        .withColumn(column, convert_udf(column))\
        .filter(F.col(column).isNotNull())
        #.withColumn(column, F.lit(F.col(column)).cast(IntegerType()))
    return cleaned_df

def clean_lead_time_days(df:DataFrame, column:str) -> DataFrame:
    cleaned_df = clean_int(df, column)\
        .where(F.col(column) > 0)
    return cleaned_df

def standardize_datetime(df: DataFrame, column: str) -> DataFrame:
    cleaned_df = df.withColumn(f"{column}_raw", F.col(column))
    cleaned_df = remove_na(cleaned_df, column)\
        .withColumn(column, F.regexp_replace(F.col(column), "/", "-"))

    parsed_ts = F.coalesce(
        F.try_to_timestamp(F.col(column), F.lit("yyyy-MM-dd HH:mm:ss")),
        F.try_to_timestamp(F.col(column), F.lit("yyyy-dd-MM HH:mm:ss"))
    )

    cleaned_df = cleaned_df\
        .withColumn(column, parsed_ts)\
        .filter(F.col(column).isNotNull())
    return cleaned_df

def standardize_date(df: DataFrame, column: str) -> DataFrame:
    cleaned_df = df.withColumn(f"{column}_raw", F.col(column))
    cleaned_df = remove_na(cleaned_df, column)\
        .withColumn(column, F.regexp_replace(F.col(column), "/", "-"))\

    parsed_date = F.coalesce(
        F.try_to_timestamp(F.col(column), F.lit("dd-MM-yyyy")),
        F.try_to_timestamp(F.col(column), F.lit("yyyy-MM-dd")),
    )

    cleaned_df = cleaned_df\
        .withColumn(column, F.to_date(parsed_date))\
        .filter(F.col(column).isNotNull())
    return cleaned_df

def clean_ids(df:DataFrame, column:str, threshold:int=100) -> DataFrame:
    col = F.abs(F.col(column))
    cleaned_df = clean_int(df, column)\
        .where(col < threshold)
    return cleaned_df
