import os
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import BooleanType, IntegerType, FloatType

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
    cleaned_df = df.withColumn(column,
                        F.regexp_extract(F.col(column),
                                        r"(\d+)", 1)
    )
    return cleaned_df

def clean_is_active_types(df:DataFrame, column:str="is_active") -> DataFrame:
    cleaned_df = trim_lower_column(df, column)
    cleaned_df = clean_boolean_types(cleaned_df, column)
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

def clean_decimal(df:DataFrame, column:str) -> DataFrame:
    cleaned_df = df \
        .withColumn(column, F.regexp_replace(F.col(column), r",", ".")) \
        .withColumn(column, F.lit(F.col(column)).cast(FloatType()))
    return cleaned_df

def clean_int(df:DataFrame, column:str) -> DataFrame:
    cleaned_df = df \
        .withColumn(column, F.regexp_replace(F.col(column), r",", ".")) \
        .withColumn(column, F.lit(F.col(column)).cast(IntegerType()))
    return cleaned_df