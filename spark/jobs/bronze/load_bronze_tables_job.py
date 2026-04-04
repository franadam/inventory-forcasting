import os
import logging
from pyspark.sql import DataFrame, SparkSession

from spark.common.config import BRONZE_PATH
from spark.common.database_utils import create_tables
from spark.common.data_loading import read_csv, read_postgresql_table, save_into_db
from spark.common.spark_session import build_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_bronze_tables() -> None:
    try:
        logger.info("Starting creation of bronze tables")
        create_tables()
        logger.info("Bronze tables created successfully")

    except Exception as e:
        logger.error(f"An error occurred during the creation of bronze tables: {e}")
        raise

def ingest_postgresql_table(spark:SparkSession, input_path:str, schema:str, table:str) -> None:
    logger.info(f"starting population of the {table} table in the {schema} schema.")
    df = read_csv(spark, input_path)
    save_into_db(schema=schema, table=table,dataframe=df)
    logger.info( f"{table} table in the {schema} schema populated successfully")

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
            logger.info(f"starting to populate bronze_{table} with {file_name} file")
            df = read_csv(spark=spark, path=file_path)
            save_into_db(schema='bronze', table=table, dataframe=df)
            logger.info(f"bronze {table} table was populated successfully")
    except Exception as e:
        logger.error(f"An error occurred during the population of bronze tables: {e}")
        raise    
    finally:
        spark.stop()            

def job():
    populate_bronze_tables()

if __name__ == "__main__":
    job()