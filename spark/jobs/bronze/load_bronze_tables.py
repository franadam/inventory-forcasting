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
    logger.info("populate_bronze_tables not implemented yet")
    spark = build_spark_session(app_name="populate_bronze_tables")
    try:
        for elem in os.scandir(BRONZE_PATH):
            if elem.is_file() and elem.name.endswith(".csv"):
                file_path = elem.path
                file_name = os.path.basename(file_path)
                table = file_name.split('.')[0]
                logger.info(f"starting to populate bronze_{table} with {file_name} file")
                df = read_csv(spark=spark, path=file_path)
                save_into_db(schema='bronze', table=table,dataframe=df)
                logger.info(f"bronze {table} table was populated successfully")
    except Exception as e:
        logger.error(f"An error occurred during the population of bronze tables: {e}")
        raise    
    finally:
        spark.stop()            

if __name__ == "__main__":
    spark = build_spark_session(app_name="data_loading")
    input_path = os.path.join(BRONZE_PATH, "customers.csv")
    logger.info("read customers data from db")
    ingest_postgresql_table(spark, input_path,'bronze', 'costumers')
    df = read_postgresql_table(spark, 'bronze', 'costumers')
    df.show()
