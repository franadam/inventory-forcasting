from pyspark.sql import SparkSession

from spark.common.config import APP_NAME
from spark.common.spark_config import DEFAULT_SPARK_CONF


def build_spark_session(
    app_name: str ,
    extra_conf: dict[str, str] | None = None,
) -> SparkSession:
    """
    Build and return a SparkSession with the default project configuration.
    Extra Spark configuration can be injected with extra_conf.
    """
    final_app_name = app_name or APP_NAME
    conf = DEFAULT_SPARK_CONF.copy()

    if extra_conf:
        conf.update(extra_conf)

    builder = SparkSession.builder.appName(final_app_name)

    for key, value in conf.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark