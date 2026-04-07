from datetime import datetime, timedelta

from spark.common.config import POSTGRES_CONN_ID, POSTGRES_HOST, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_PORT 

# Default Args
DEFAULT_AIRFLOW_ARGS = {
    "owner": "franadam",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "franadam.data@engineers.com",
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2025, 1, 1)
}

POSTGRES_CONNECTION_ARGS = {
    "conn_id": POSTGRES_CONN_ID,
    "host": POSTGRES_HOST,
    "schema": "staging",
    "login": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "port": POSTGRES_PORT,
}