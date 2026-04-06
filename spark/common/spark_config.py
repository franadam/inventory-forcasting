from spark.common.config import (
    POSTGRES_JDBC_JAR,
    PYTHONPATH_VALUE,
    SPARK_DEPLOY_MODE,
    SPARK_MASTER_URL,
    POSTGRES_JDBC_JAR,
    SPARK_CONN_ID
)

DEFAULT_SPARK_CONF = {
    "spark.master": SPARK_MASTER_URL,
    "spark.submit.deployMode": SPARK_DEPLOY_MODE,
    "spark.driver.extraClassPath": POSTGRES_JDBC_JAR,
    "spark.executor.extraClassPath": POSTGRES_JDBC_JAR,
    "spark.executorEnv.PYTHONPATH": PYTHONPATH_VALUE,
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
}