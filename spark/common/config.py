import os

ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")

PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/project")

BRONZE_PATH = os.getenv("BRONZE_PATH", f"{PROJECT_ROOT}/data/bronze")
