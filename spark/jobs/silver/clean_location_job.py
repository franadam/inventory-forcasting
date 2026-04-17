import logging

from pyspark.sql import DataFrame

from spark.common.spark_session import build_spark_session
from spark.common.dataframe_utils import read_postgresql_table, save_into_db
from spark.common.clean_utils import clean_is_active_types, clean_city, clean_region, clean_address, standardize_postal_code
from spark.transformations.silver.locations import clean_location_name, clean_location_type, standardize_location_code

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def clean_locations() -> DataFrame:
    spark = build_spark_session('clean_locations')
    df = read_postgresql_table(spark=spark, schema='bronze', table='locations')
    cleaned_df = clean_location_type(df)
    cleaned_df = clean_city(cleaned_df)
    cleaned_df = clean_location_name(cleaned_df)
    cleaned_df = clean_is_active_types(cleaned_df)
    cleaned_df = clean_region(cleaned_df)
    cleaned_df = clean_address(cleaned_df)
    cleaned_df = standardize_postal_code(cleaned_df)
    cleaned_df = standardize_location_code(cleaned_df)

    # save_into_db(schema='silver', table='locations', dataframe=cleaned_df, mode='append')
    save_into_db(schema='staging', table='locations', dataframe=cleaned_df)
    return cleaned_df


if __name__ == "__main__":
    clean_locations()
