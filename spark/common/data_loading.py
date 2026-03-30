import os
from spark_session import spark
from pyspark.sql import functions as F
import logging

from spark.common.config import BRONZE_PATH

logger = logging.getLogger(__name__)

def read_csv(path: str):
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
        .withColumn("raw_file_path", F.input_file_name())
    )
    return df

if __name__ == "__main__":
    df = read_csv(os.path.join(BRONZE_PATH, "customers.csv"))
    logger.warning("Preview of customers.csv")
    df.show(20, truncate=False)
    df.printSchema()