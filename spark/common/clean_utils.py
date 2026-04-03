import os
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

def trim_lower_column(df:DataFrame, column:str) -> DataFrame:
    cleaned_df = (df
                    .withColumn(column,
                        F.trim(F.col(column)))
                    .withColumn(column, F.lower(column))
    )
    return cleaned_df

def clean_boolean_types(df:DataFrame, column:str) -> DataFrame:
    cleaned_df = trim_lower_column(df, column)\
        .withColumn(column,
                        F.initcap(F.col(column)))\
        .withColumn(column, F.lit(F.col(column)).cast('boolean'))
    return cleaned_df