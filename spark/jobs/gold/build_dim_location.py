import logging

from pyspark.sql import DataFrame

from spark.common.spark_session import build_spark_session
from spark.common.data_loading import save_into_db
from spark.transformations.gold.dim_location import (build_province_region_map, reorder_dim_location_columns, 
                                                        add_dim_location_surrogate_key, prepare_customer_locations, 
                                                        prepare_internal_locations, enrich_location_geography)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def build_dim_location() -> DataFrame:
    spark = build_spark_session("build_dim_location")

    customer_locations_df = prepare_customer_locations(spark)
    silver_locations_df = prepare_internal_locations(spark)
    combined_df = customer_locations_df.unionByName(silver_locations_df)

    province_region_map = build_province_region_map()

    dim_location_df = combined_df\
        .transform(lambda df: enrich_location_geography(df, province_region_map))\
        .transform(add_dim_location_surrogate_key)\
        .transform(reorder_dim_location_columns)
    
    save_into_db(schema='gold', table='dim_location', dataframe=dim_location_df)

    return dim_location_df
    
if __name__ == "__main__":
    build_dim_location()