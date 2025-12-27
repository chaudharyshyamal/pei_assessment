# Databricks notebook source
# MAGIC %run ../utils/helpers.ipynb

# COMMAND ----------

import pytest


@pytest.mark.parametrize(
    "path,expected_format,is_excel",
    [
        ("data/file.csv",  "csv",  False),
        ("data/file.json", "json", False),
        ("data/file.xlsx", None,   True),
    ]
)
def test_read_various_formats(path, expected_format, is_excel, patch_spark, mock_df):
    df = read_volume_files(path, column_mapping_dict={"A": "a"})

    assert df is mock_df

    if is_excel:
        # Excel uses read.options(...).excel(...)
        patch_spark.read.options.assert_called_with(
            header="true", inferSchema="true"
        )
        patch_spark.read.options.return_value.excel.assert_called_once_with(path)

    else:
        # CSV / JSON uses read.format(...).options(...).load(...)
        patch_spark.read.format.assert_called_with(expected_format)
        patch_spark.read.format.return_value.options.assert_called_with(
            header="true", inferSchema="true"
        )
        patch_spark.read.format.return_value.options.return_value.load.\
            assert_called_once_with(path)

    mock_df.withColumnsRenamed.assert_called_once_with({"A": "a"})
