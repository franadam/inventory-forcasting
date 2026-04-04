from spark.jobs.bronze.load_bronze_tables_job import ingest_postgresql_table

def test_ingest_postgresql_table_writes_rows(
    spark,
    sample_locations_csv,
    postgres_conn,
    setup_test_table,
):
    test_schema = setup_test_table
    table_name = "locations"

    ingest_postgresql_table(
        spark=spark,
        input_path=sample_locations_csv,
        schema=test_schema,
        table=table_name,
    )

    with postgres_conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {test_schema}.{table_name};")
        row_count = cur.fetchone()[0]

        cur.execute(f"""
            SELECT location_id, location_code, city
            FROM {test_schema}.{table_name}
            ORDER BY location_id
        """)
        rows = cur.fetchall()

    assert row_count == 3
    assert rows[0] == (1, "LOC001", "Brussels")
    assert rows[1] == (2, "LOC002", "Antwerp")
    assert rows[2] == (3, "LOC003", "Ghent")