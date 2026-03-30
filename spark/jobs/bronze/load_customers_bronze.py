import logging
from spark.common.database_utils import create_tables

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