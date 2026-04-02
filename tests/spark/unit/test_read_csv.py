from pyspark.sql import functions as F

from spark.jobs.bronze.load_bronze_tables import read_csv

def test_read_csv(spark, sample_locations_csv):
    df = read_csv(
        spark=spark,
        path=sample_locations_csv
    )

    # 1. verify the DataFrame is not empty
    assert df.count() == 3

    # 2. verify expected columns
    expected_columns = {
        "location_id",
        "location_code",
        "location_name",
        "location_type",
        "city",
        "raw_file_path"
    }

    assert set(df.columns) == expected_columns

    # 3. verify specific data
    row = df.filter(F.col("location_id") == 1).collect()[0]

    assert row["location_code"] == "LOC001"
    assert row["city"] == "Brussels"

    # 4. verify absence of null 
    assert df.filter(F.col("location_id").isNull()).count() == 0