import os

ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")

# Project params
APP_NAME = os.getenv("SPARK_APP_NAME", "inventory_forecasting")
PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/project")
SQL_ROOT = os.getenv("SQL_ROOT", "/opt/project/sql")
SPARK_JOBS_ROOT = os.getenv("SPARK_JOBS_ROOT", f"{PROJECT_ROOT}/spark/jobs")

# Postgres connection 
POSTGRES_HOST = os.getenv("POSTGRES_CONN_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_CONN_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_CONN_USERNAME")
POSTGRES_PASSWORD = os.getenv("POSTGRES_CONN_PASSWORD")
POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "inventory_db")

# Spark params
POSTGRES_JDBC_JAR = os.getenv("POSTGRES_JDBC_JAR")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL")
SPARK_DEPLOY_MODE = os.getenv("SPARK_DEPLOY_MODE")
PYTHONPATH_VALUE = os.getenv("PYTHONPATH")
SPARK_CONN_ID = os.getenv("SPARK_CONN_ID")

# Paths
BRONZE_PATH = os.getenv("BRONZE_PATH", f"{PROJECT_ROOT}/data/bronze")
SILVER_PATH = os.getenv("SILVER_PATH", f"{PROJECT_ROOT}/data/silver")
GOLD_PATH = os.getenv("GOLD_PATH", f"{PROJECT_ROOT}/data/gold")
REJECTED_PATH = os.getenv("REJECTED_PATH", f"{PROJECT_ROOT}/data/rejected")

