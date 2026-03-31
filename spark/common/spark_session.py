from pyspark.sql import SparkSession

SPARK_JOB_ROOT = "/opt/project/spark/jobs"
POSTGRES_JDBC_JAR = "/opt/spark/jars/postgresql-42.7.8.jar"

SPARK_CONF = {
    "spark.master": "spark://spark-master:7077",
    "spark.submit.deployMode": "client",
    "spark.driver.extraClassPath": POSTGRES_JDBC_JAR,
    "spark.executor.extraClassPath": POSTGRES_JDBC_JAR,
    "spark.executorEnv.PYTHONPATH": "/opt/project:/opt/project/spark",
    "spark.log.level":"WARN"
}

spark = SparkSession\
    .builder\
    .appName("inventory_forcasting")\
    .getOrCreate()