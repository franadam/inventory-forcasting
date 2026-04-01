def test_postgres_connection(mock_postgres_connection_var): #argument is the function defined in the confest file
    conn = mock_postgres_connection_var
    assert conn.login == "mock_username"
    assert conn.password == "mock_password"
    assert conn.host == "mock_host"
    assert conn.port == 1234
    assert conn.schema == "mock_db_name"
