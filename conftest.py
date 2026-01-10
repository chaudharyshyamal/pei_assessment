import pytest
import os
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    os.environ["PYSPARK_PYTHON"] = "python"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

    return (
        SparkSession.builder
        .master("local[1]")
        .appName("pytest-spark")
        .getOrCreate()
    )
