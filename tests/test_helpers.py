# Databricks notebook source
import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import DataFrame
import os
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType, DateType

# COMMAND ----------

from utils.helpers import read_volume_files, cast_columns, raw_date_write

# COMMAND ----------

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()

# COMMAND ----------

@pytest.fixture
def mock_df():
    df = MagicMock(spec=DataFrame)
    df.withColumnsRenamed.return_value = df
    return df


@pytest.fixture
def default_options():
    return {"header": "true", "inferSchema": "true"}


@pytest.mark.parametrize(
    "path,file_format",
    [
        ("/Volumes/data/customers.csv", "csv"),
        ("/Volumes/data/customers.json", "json"),
    ],
)
def test_read_volume_files_csv_json(
    spark, mock_df, default_options, path, file_format
):
    mock_reader = MagicMock()
    mock_reader.format.return_value = mock_reader
    mock_reader.options.return_value = mock_reader
    mock_reader.load.return_value = mock_df

    with patch("your_module.spark.read", mock_reader):
        result_df = read_volume_files(path, column_mapping_dict={"A": "a"})

    mock_reader.format.assert_called_once_with(file_format)
    mock_reader.options.assert_called_once_with(**default_options)
    mock_reader.load.assert_called_once_with(path)

    mock_df.withColumnsRenamed.assert_called_once_with({"A": "a"})
    assert result_df == mock_df


def test_read_volume_files_xlsx(spark, mock_df, default_options):
    mock_reader = MagicMock()
    mock_reader.options.return_value = mock_reader
    mock_reader.excel.return_value = mock_df

    with patch("your_module.spark.read", mock_reader):
        result_df = read_volume_files(
            "/Volumes/data/file.xlsx",
            column_mapping_dict={"B": "b"},
            extraOpt="x"
        )

    # default + override options
    expected_options = default_options.copy()
    expected_options["extraOpt"]()_

# COMMAND ----------

@pytest.fixture
def sample_df(spark):
    return spark.createDataFrame(
        [
            Row(id="1", value="10"),
            Row(id="2", value="20"),
        ]
    )


@pytest.fixture
def target_schema():
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("value", IntegerType(), True),
    ])


def test_cast_columns_schema(sample_df, target_schema):
    result_df = cast_columns(sample_df, target_schema)

    assert result_df.schema == target_schema


@pytest.mark.parametrize(
    "row_index,expected",
    [
        (0, (1, 10)),
        (1, (2, 20)),
    ],
)
def test_cast_columns_values(sample_df, target_schema, row_index, expected):
    result_df = cast_columns(sample_df, target_schema)

    rows = result_df.collect()
    assert tuple(rows[row_index]) == expected

