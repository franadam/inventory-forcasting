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

def read_postgresql_table(spark:SparkSession, schema: str, table: str) -> DataFrame:
        jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        df = (
            spark.read 
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", f"{schema}.{table}")
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .load()
        )
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
    spark = build_spark_session(app_name="data_loading")
    df = read_csv(spark,os.path.join(BRONZE_PATH, "customers.csv"))
    logger.info("Preview of customers.csv")
    df.show(5, truncate=False)
    df.printSchema()
    logger.info("save customers data into db")
    save_into_db(schema='bronze', table='customers',dataframe=df)
    logger.info("read customers data from db")
    df_table = read_postgresql_table(spark, schema='bronze', table='customers')
    df_table.show(5)