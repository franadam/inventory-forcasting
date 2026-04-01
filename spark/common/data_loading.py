import os
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession
import logging

from spark.common.spark_session import build_spark_session
from spark.common.config import BRONZE_PATH, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_USER, POSTGRES_HOST, POSTGRES_PORT

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_csv(spark: SparkSession, path: str, infer_schema: bool = True) -> DataFrame:
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", infer_schema)
        .csv(path)
        .withColumn("raw_file_path", F.input_file_name())
    )
    logger.info(f"CSV loaded from {path}")
    return df



def save_into_db(schema: str, table: str, dataframe: DataFrame, mode: str = "overwrite") -> None:
    jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    (
        dataframe.write
        .format("jdbc")
        .mode(mode)
        .option("url", jdbc_url)
        .option("dbtable", f"{schema}.{table}")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .save()
    )
    logger.info(f"Data written to {schema}.{table}")

if __name__ == "__main__":
    df = read_csv(os.path.join(BRONZE_PATH, "customers.csv"))
    logger.warning("Preview of customers.csv")
    df.show(5, truncate=False)
    df.printSchema()
    logger.warning("save customers data into db")
    save_into_db(schema='bronze', table='customers',dataframe=df)