import logging
import os
from spark.common.database_utils import create_tables
from spark.common.data_loading import read_csv, save_into_db
from spark.common.config import BRONZE_PATH

logger = logging.getLogger(__name__)

def create_bronze_tables():
    try:
        logger.info("Starting creation of bronze tables")
        create_tables()
        logger.info("Bronze tables created successfully")

    except Exception as e:
        logger.error(f"An error occurred during the creation of bronze tables: {e}")
        raise


def populate_bronze_tables():
    logger.info("populate_bronze_tables not implemented yet")
    try:
        for elem in os.scandir(BRONZE_PATH):
            if elem.is_file():
                file_path = elem.path
                file_name = file_path.split('/')[-1]
                table = file_name.split('.')[0]
                logger.info(f"starting to populate bronze_{table} with {file_name} file")
                df = read_csv(file_path)
                save_into_db(schema='bronze', table=table,dataframe=df)
                logger.info(f"bronze {table} table was populated")
    except Exception as e:
        logger.error(f"An error occurred during the population of bronze tables: {e}")
        raise                

if __name__ == "__main__":
    populate_bronze_tables()
