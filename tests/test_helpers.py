import pytest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, IntegerType, DoubleType
from utils.helpers import read_volume_files, cast_columns, raw_date_write, aggregate_calculation
from pyspark.sql.functions import sum, avg, col


# Unit test - read_volume_files
@pytest.fixture
def mock_df():
    df = MagicMock()
    df.withColumnsRenamed.return_value = df
    return df


@pytest.fixture
def mock_spark_reader(monkeypatch, mock_df):
    """
    Patch SparkSession.read (read-only property) at CLASS level
    """

    mock_reader = MagicMock()
    mock_reader.format.return_value = mock_reader
    mock_reader.options.return_value = mock_reader
    mock_reader.load.return_value = mock_df

    monkeypatch.setattr(
        SparkSession,
        "read",
        property(lambda self: mock_reader)
    )

    return mock_reader


@pytest.mark.parametrize(
    "file_path, expected_format",
    [
        ("data/input.csv", "csv"),
        ("data/input.json", "json"),
        ("data/input.xlsx", "excel"),
    ]
)
def test_read_volume_files_with_magicmock(
    spark,
    mock_spark_reader,
    file_path,
    expected_format
):
    column_mapping = {"Emp Name": "emp_name", "Salary": "salary"}

    result_df = read_volume_files(
        spark=spark,
        volume_path=file_path,
        column_mapping_dict=column_mapping,
        delimiter=","  # extra option
    )

    mock_spark_reader.format.assert_called_once_with(expected_format)
    mock_spark_reader.options.assert_called_once()
    mock_spark_reader.load.assert_called_once_with(file_path)

    result_df.withColumnsRenamed.assert_called_once_with(column_mapping)


def test_unsupported_file_format_raises_error(spark):
    with pytest.raises(ValueError, match="Unsupported file format"):
        read_volume_files(spark=spark, volume_path="dbfs:/volumes/test/file.parquet")



# Unit test - cast_columns
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



# Unit test - raw_date_write
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




# Unit test - aggregate_calculation
@pytest.mark.parametrize(
    "agg_func, order_cols, expected_data",
    [
        (
            sum,
            [("total_salary", "desc")],
            [("IT", 300.0), ("HR", 200.0)]
        ),
        (
            avg,
            [("total_salary", "asc")],
            [("HR", 200.0), ("IT", 150.0)]
        ),
    ]
)
def test_aggregate_calculation_df_equal(
    spark,
    agg_func,
    order_cols,
    expected_data
):
    # Input DataFrame
    input_df = spark.createDataFrame(
        [
            ("IT", 100.0),
            ("IT", 200.0),
            ("HR", 200.0),
        ],
        ["dept", "salary"]
    )

    # Result DataFrame
    result_df = aggregate_calculation(
        df=input_df,
        groupby_cols=["dept"],
        agg_col="salary",
        agg_func=agg_func,
        round_upto=2,
        alias_col_name="total_salary",
        order_cols=order_cols
    )

    # Expected DataFrame (from parametrize)
    expected_df = spark.createDataFrame(
        expected_data,
        ["dept", "total_salary"]
    )

    assert result_df.filter(col("dept") == "IT").select("total_salary").collect()[0][0] == expected_df.filter(col("dept") == "IT").select("total_salary").collect()[0][0]
    assert result_df.schema == expected_df.schema