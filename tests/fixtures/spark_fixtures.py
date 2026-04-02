import pytest
from pyspark.sql import SparkSession
import tempfile
import shutil
import os

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("pytest-spark")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def temp_dir():
    dirpath = tempfile.mkdtemp()
    yield dirpath
    shutil.rmtree(dirpath)


@pytest.fixture
def sample_locations_csv(temp_dir):
    file_path = os.path.join(temp_dir, "locations.csv")

    data = """location_id,location_code,location_name,location_type,city
1,LOC001,Brussels Central,sanicenter,Brussels
2,LOC002,Antwerp North,regional_warehouse,Antwerp
3,LOC003,Ghent Store,sanicenter,Ghent
"""

    with open(file_path, "w") as f:
        f.write(data)

    return file_path