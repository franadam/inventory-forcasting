import os
from spark.common.spark_session import spark
from pyspark.sql import functions as F
import logging

from spark.common.config import BRONZE_PATH, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_USER, POSTGRES_HOST, POSTGRES_PORT

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

def save_into_db(schema:str, table:str,dataframe):
    (
        dataframe.write
        .format("jdbc")
        .mode("overwrite")
        .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
        .option("dbtable", f"{schema}.{table}")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .save()
    )

if __name__ == "__main__":
    df = read_csv(os.path.join(BRONZE_PATH, "customers.csv"))
    logger.warning("Preview of customers.csv")
    df.show(5, truncate=False)
    df.printSchema()
    logger.warning("save customers data into db")
    save_into_db(schema='bronze', table='customers',dataframe=df)