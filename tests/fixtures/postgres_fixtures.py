import uuid
import pytest
import psycopg2

from spark.common.config import POSTGRES_HOST,POSTGRES_PORT,POSTGRES_USER,POSTGRES_PASSWORD, POSTGRES_DB

@pytest.fixture
def test_schema():
    return f"test_bronze_{uuid.uuid4().hex[:8]}"

@pytest.fixture
def postgres_conn():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    yield conn
    conn.close()

@pytest.fixture
def setup_test_table(postgres_conn, test_schema):
    with postgres_conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {test_schema};")
        cur.execute(f"""
            CREATE TABLE {test_schema}.locations (
                location_id INTEGER,
                location_code TEXT,
                location_name TEXT,
                location_type TEXT,
                city TEXT,
                raw_file_path TEXT
            );
        """)
        postgres_conn.commit()

    yield test_schema

    with postgres_conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {test_schema} CASCADE;")
        postgres_conn.commit()