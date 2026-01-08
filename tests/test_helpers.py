import pytest
from unittest.mock import MagicMock
from pyspark.sql.types import StructField, StringType, IntegerType, DoubleType
from utils.helpers import read_volume_files, cast_columns, raw_date_write

@pytest.fixture
def mock_df():
    df = MagicMock()
    df.withColumnsRenamed.return_value = df
    return df


@pytest.fixture
def mock_spark_reader(monkeypatch, mock_df):
    mock_reader = MagicMock()
    mock_reader.format.return_value = mock_reader
    mock_reader.options.return_value = mock_reader
    mock_reader.load.return_value = mock_df

    mock_spark = MagicMock()
    mock_spark.read = mock_reader

    monkeypatch.setattr("utils.helpers.spark", mock_spark)

    return mock_reader


@pytest.mark.parametrize(
    "file_path,expected_format",
    [
        ("dbfs:/volumes/test/file.csv", "csv"),
        ("dbfs:/volumes/test/file.json", "json"),
        ("dbfs:/volumes/test/file.xlsx", "excel"),
    ],
)
def test_read_volume_files_supported_formats(
    mock_spark_reader, mock_df, file_path, expected_format
):
    column_mapping = {"First Name": "first_name"}

    result = read_volume_files(
        volume_path=file_path,
        column_mapping_dict=column_mapping,
        inferSchema="false"
    )

    mock_spark_reader.format.assert_called_with(expected_format)
    mock_spark_reader.options.assert_called()
    mock_spark_reader.load.assert_called_with(file_path)

    mock_df.withColumnsRenamed.assert_called_once_with(column_mapping)
    assert result == mock_df


def test_unsupported_file_format_raises_error():
    with pytest.raises(ValueError, match="Unsupported file format"):
        read_volume_files("dbfs:/volumes/test/file.parquet")



@pytest.fixture
def input_df(spark):
    data = [
        ("1", "10.5", "abc"),
        ("2", "20.0", "xyz"),
    ]
    return spark.createDataFrame(
        data,
        ["id", "amount", "name"]
    )


@pytest.mark.parametrize(
    "schema,expected_types",
    [
        (
            [
                StructField("id", IntegerType()),
                StructField("amount", DoubleType()),
                StructField("name", StringType()),
            ],
            {
                "id": "int",
                "amount": "double",
                "name": "string",
            },
        ),
        (
            [
                StructField("id", StringType()),
                StructField("amount", StringType()),
                StructField("name", StringType()),
            ],
            {
                "id": "string",
                "amount": "string",
                "name": "string",
            },
        ),
    ],
)
def test_cast_columns_success(input_df, schema, expected_types):
    result_df = cast_columns(input_df, schema)

    result_schema = {
        field.name: field.dataType.simpleString()
        for field in result_df.schema.fields
    }

    assert result_schema == expected_types



@pytest.fixture
def mock_write_df():
    df = MagicMock()

    writer = MagicMock()
    writer.format.return_value = writer
    writer.mode.return_value = writer

    df.write = writer
    return df


@pytest.mark.parametrize(
    "table_name",
    [
        "default.raw_orders",
        "catalog.schema.raw_customers",
    ],
)
def test_raw_date_write_success(mock_write_df, table_name):
    raw_date_write(mock_write_df, table_name)

    mock_write_df.write.format.assert_called_once_with("delta")
    mock_write_df.write.mode.assert_called_once_with("overwrite")
    mock_write_df.write.saveAsTable.assert_called_once_with(table_name)
