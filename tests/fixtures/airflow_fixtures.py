import pytest
from unittest import mock
from airflow.models.connection import Connection

@pytest.fixture
def mock_postgres_connection_var():
    conn = Connection(
        conn_id="INVENTORY_DB",
        conn_type="postgres",
        login="mock_username",
        password="mock_password",
        host="mock_host",
        port=1234,
        schema="mock_db_name"
    )
    conn_uri = conn.get_uri()

    with mock.patch.dict(
        "os.environ",
        {"AIRFLOW_CONN_INVENTORY_DB": conn_uri}
    ):
        yield Connection.get_connection_from_secrets("INVENTORY_DB")