import logging

from pyspark.sql import DataFrame

from spark.common.spark_session import build_spark_session
from spark.common.dataframe_utils import save_into_db, read_postgresql_table
from spark.transformations.gold.dim_location import reorder_dim_location_columns, prepare_internal_locations

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_dim_location() -> DataFrame:
    spark = build_spark_session("build_dim_location")

    locations_df = prepare_internal_locations(spark)

    dim_location_df = locations_df\
        .transform(reorder_dim_location_columns)

    save_into_db(schema='gold', table='dim_location',
                 dataframe=dim_location_df, mode="append")

    logger.info("Columns in locations_df: %s", locations_df.columns)
    return dim_location_df


if __name__ == "__main__":
    build_dim_location()