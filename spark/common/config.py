import os

ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")

# Postgres connection 
POSTGRES_HOST = os.getenv("POSTGRES_CONN_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_CONN_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_CONN_USERNAME")
POSTGRES_PASSWORD = os.getenv("POSTGRES_CONN_PASSWORD")
POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_IDo", "inventory_db")

PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/project")
SQL_ROOT = os.getenv("SQL_ROOT", "/opt/project/sql")

BRONZE_PATH = os.getenv("BRONZE_PATH", f"{PROJECT_ROOT}/data/bronze")
SILVER_PATH = os.getenv("SILVER_PATH", f"{PROJECT_ROOT}/data/silver")
GOLD_PATH = os.getenv("GOLD_PATH", f"{PROJECT_ROOT}/data/gold")
REJECTED_PATH = os.getenv("REJECTED_PATH", f"{PROJECT_ROOT}/data/rejected")