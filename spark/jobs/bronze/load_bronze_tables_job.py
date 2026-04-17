import os
import logging

from pyspark.sql import DataFrame, SparkSession

from spark.common.config import BRONZE_PATH

from spark.common.spark_session import build_spark_session
from spark.common.dataframe_utils import read_csv, read_postgresql_table, save_into_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def populate_bronze_tables() -> None:
    logger.info("Starting bronze population job")
    logger.info(f"BRONZE_PATH = {BRONZE_PATH}")

    if not os.path.exists(BRONZE_PATH):
        raise FileNotFoundError(f"BRONZE_PATH does not exist: {BRONZE_PATH}")

    csv_files = [
        elem.path
        for elem in os.scandir(BRONZE_PATH)
        if elem.is_file() and elem.name.lower().endswith(".csv")
    ]

    logger.info("Found %s CSV file(s) in %s", len(csv_files), BRONZE_PATH)
    logger.info("Files found: %s", [os.path.basename(f) for f in csv_files])

    spark = build_spark_session(app_name="populate_bronze_tables")
    try:
        for file_path in csv_files:
            file_name = os.path.basename(file_path)
            table = os.path.splitext(file_name)[0]
            logger.info(
                f"starting to populate bronze_{table} with {file_name} file")
            df = read_csv(spark=spark, path=file_path)
            save_into_db(schema='bronze', table=table, dataframe=df)
            logger.info(f"bronze {table} table was populated successfully")
    except Exception as e:
        logger.error(
            f"An error occurred during the population of bronze tables: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    populate_bronze_tables()
