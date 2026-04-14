from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

def add_surrogate_key(df: DataFrame, surrogate_key:str, ordered_list:list) -> DataFrame:
    window_spec = Window.orderBy(*ordered_list)
    cleaned_df = df.withColumn(surrogate_key, F.row_number().over(window_spec))
    return cleaned_df

def reorder_columns(df: DataFrame, ordered_list:list) -> DataFrame:
    ordered_df = df.select(*ordered_list)
    return ordered_df