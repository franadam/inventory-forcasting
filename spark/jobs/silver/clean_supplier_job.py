import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.common.spark_session import build_spark_session
from spark.common.data_loading import read_postgresql_table, save_into_db
from spark.common.clean_utils import clean_decimal, clean_is_active_types, clean_capital_name
from spark.transformations.silver.suppliers import standardize_supplier_code, clean_supplier_email

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_suppliers() -> DataFrame:
    spark = build_spark_session('clean_suppliers')
    df = read_postgresql_table(spark=spark, schema='bronze', table='suppliers')
    cleaned_df = standardize_supplier_code(df)
    cleaned_df = clean_capital_name(cleaned_df, "contact_name")\
        .filter(F.col("contact_name").isNotNull())
    cleaned_df = cleaned_df.withColumn("supplier_name", F.trim(F.col("supplier_name")))
    cleaned_df = clean_supplier_email(cleaned_df)
    cleaned_df = clean_capital_name(cleaned_df, "country")
    cleaned_df = clean_decimal(cleaned_df, "avg_lead_time_days")
    cleaned_df = clean_decimal(cleaned_df, "reliability_score")
    cleaned_df = clean_is_active_types(cleaned_df)
    
    #save_into_db(schema='silver', table='suppliers', dataframe=cleaned_df, mode='append')
    save_into_db(schema='staging', table='suppliers', dataframe=cleaned_df)
    return cleaned_df


if __name__ == "__main__":
    clean_suppliers()
