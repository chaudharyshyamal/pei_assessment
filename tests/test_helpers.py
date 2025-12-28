# Databricks notebook source
import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import DataFrame

# COMMAND ----------

# MAGIC %run ../utils/helpers

# COMMAND ----------

@pytest.fixture
def mock_df():
    df = MagicMock(spec=DataFrame)
    df.withColumnsRenamed.return_value = df
    return df

# COMMAND ----------

@patch("utils.reader.spark")
def test_read_csv(mock_spark):
    mock_df = MagicMock(spec=DataFrame)
    mock_df.withColumnsRenamed.return_value = mock_df

    mock_reader = MagicMock()
    mock_reader.options.return_value = mock_reader
    mock_reader.format.return_value = mock_reader
    mock_reader.load.return_value = mock_df
    mock_reader.excel.return_value = mock_df

    mock_spark.read = mock_reader

    result = read_volume_files("/Volumes/demo/file.csv")

    mock_reader.format.assert_called_with("csv")
    mock_reader.options.assert_called()
    mock_reader.load.assert_called_with("/Volumes/demo/file.csv")
    mock_df.withColumnsRenamed.assert_called_once()
    assert result == mock_df
