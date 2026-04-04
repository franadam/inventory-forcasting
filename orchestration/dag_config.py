from datetime import datetime, timedelta

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