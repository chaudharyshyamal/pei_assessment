# # Databricks notebook source
# from pyspark.sql import Row
# from pyspark.sql.types import StructType, StructField, StringType
# from pyspark.sql import SparkSession
# import pytest
# from dq.rules import validate_columns, validate_primary_key_unique, null_check, dq_compare, dq_compare_columns, dq_email_check, missing_reference_check

# # COMMAND ----------

# from pyspark.sql.functions import expr, col, when, count
# from unittest.mock import MagicMock, patch

# # COMMAND ----------

# @pytest.fixture(scope="session")
# def spark():
#     return SparkSession.builder.appName("pytest").getOrCreate()

# # COMMAND ----------

# @pytest.fixture
# def sample_df(spark):
#     return spark.createDataFrame([
#         Row(id="1", amount="10.5", dt="25/8/2017"),
#         Row(id="2", amount="abc",  dt="2018-01-01"),
#         Row(id="3", amount=None,   dt="31/12/2017"),
#     ])


# @pytest.mark.parametrize(
#     "schema_map,expected_valid_ids,expected_invalid_ids",
#     [
#         (
#             {"id": "int", "amount": "double", "dt": "date"},
#             ["1", "2"],   # id type OK always, date OK both, amount OK only first row
#             ["3"],        # amount = null → invalid
#         ),
#         (
#             {"id": "int"},
#             ["1", "2", "3"],
#             [],
#         ),
#     ],
# )


# def test_validate_columns_parametrized(
#     spark,
#     sample_df,
#     schema_map,
#     expected_valid_ids,
#     expected_invalid_ids,
# ):

#     valid_df, invalid_df = validate_columns(sample_df, schema_map)

#     valid_ids = [r["id"] for r in valid_df.collect()]
#     invalid_ids = [r["id"] for r in invalid_df.collect()]

#     assert sorted(valid_ids) == sorted(expected_valid_ids)
#     assert sorted(invalid_ids) == sorted(expected_invalid_ids)


# def test_validate_columns_date_formats(spark):

#     df = spark.createDataFrame([
#         Row(id="1", dt="1/1/2019"),
#         Row(id="2", dt="2019-02-01"),
#         Row(id="3", dt="bad-date"),
#     ])

#     schema_map = {"dt": "date"}

#     valid_df, invalid_df = validate_columns(df, schema_map)

#     assert valid_df.count() == 2
#     assert invalid_df.count() == 1

#     assert [r["id"] for r in invalid_df.collect()] == ["3"]
	

# def test_expr_called_for_casts(spark, sample_df):

#     schema_map = {"amount": "double"}

#     with patch("your_module.expr", wraps=expr) as mock_expr:
#         validate_columns(sample_df, schema_map)

#         # expr should be called at least once
#         assert mock_expr.called


# # COMMAND ----------

# @pytest.fixture
# def df_with_duplicates(spark):
#     return spark.createDataFrame([
#         Row(id=1, name="A"),
#         Row(id=1, name="B"),
#         Row(id=2, name="C"),
#     ])


# @pytest.fixture
# def df_without_duplicates(spark):
#     return spark.createDataFrame([
#         Row(id=1, name="A"),
#         Row(id=2, name="B"),
#     ])


# @pytest.mark.parametrize(
#     "primary_key,fixture_name,expect_string,expected_count",
#     [
#         (["id"], "df_with_duplicates", False, 2),   # 2 duplicate rows returned
#         (["id"], "df_without_duplicates", True, 0), # returns string
#     ],
# )
# def test_validate_primary_key_unique_parametrized(
#     request,
#     primary_key,
#     fixture_name,
#     expect_string,
#     expected_count,
# ):

#     df = request.getfixturevalue(fixture_name)

#     result = validate_primary_key_unique(df, primary_key)

#     if expect_string:
#         assert result == "No duplicates found"
#     else:
#         assert result.count() == expected_count
#         assert set(result.columns) == set(df.columns)


# def test_validate_primary_key_unique_composite_key(spark):

#     df = spark.createDataFrame([
#         Row(id=1, sub=1),
#         Row(id=1, sub=1),
#         Row(id=1, sub=2),
#     ])

#     result = validate_primary_key_unique(df, ["id", "sub"])

#     assert result.count() == 2

# # COMMAND ----------

# @pytest.fixture
# def df_with_nulls(spark):
#     return spark.createDataFrame([
#         Row(id=1,  name="A"),
#         Row(id=2,  name=None),
#         Row(id=None, name="B"),
#     ])


# @pytest.fixture
# def df_without_nulls(spark):
#     return spark.createDataFrame([
#         Row(id=1, name="A"),
#         Row(id=2, name="B"),
#     ])


# @pytest.mark.parametrize(
#     "fixture_name,expected_columns,expected_counts",
#     [
#         ("df_with_nulls", ["id", "name"], {"id": 1, "name": 1}),
#         ("df_without_nulls", [], {}),
#     ],
# )
# def test_null_check_parametrized(request, fixture_name, expected_columns, expected_counts):

#     df = request.getfixturevalue(fixture_name)

#     result_df = null_check(df)

#     # If no null columns -> select(*) returns empty
#     assert result_df.columns == expected_columns

#     if expected_columns:
#         row = result_df.first().asDict()
#         assert row == expected_counts
#     else:
#         # still should return a single row
#         assert result_df.count() == 1


# def test_null_check_single_column(spark):

#     df = spark.createDataFrame([
#         Row(x=None),
#         Row(x=1),
#     ])

#     result = null_check(df)

#     assert result.columns == ["x"]
#     assert result.first()["x"] == 1


# # COMMAND ----------

# @pytest.mark.parametrize(
#     "operator,value,expected_ids",
#     [
#         (">", 10,  [2, 3]),
#         ("<", 10,  [1]),
#         (">=", 10, [2, 3]),
#         ("<=", 10, [1, 2]),
#         ("==", 10, [2]),
#         ("!=", 10, [1, 3]),
#     ],
# )
# def test_dq_compare_parametrized(spark, operator, value, expected_ids):

#     df = spark.createDataFrame([
#         Row(id=1, amount=5),
#         Row(id=2, amount=10),
#         Row(id=3, amount=15),
#     ])

#     condition = dq_compare("amount", operator, value)

#     result = df.filter(condition).select("id").collect()

#     ids = [r.id for r in result]

#     assert sorted(ids) == sorted(expected_ids)


# def test_dq_compare_invalid_operator():
#     with pytest.raises(ValueError) as e:
#         dq_compare("amount", "??", 10)

#     assert "Invalid operator" in str(e.value)

# # COMMAND ----------

# @pytest.fixture
# def sample_df(spark):
#     return spark.createDataFrame([
#         Row(id=1, amount=5,   threshold=10),
#         Row(id=2, amount=10,  threshold=10),
#         Row(id=3, amount=15,  threshold=10),
#     ])


# @pytest.mark.parametrize(
#     "operator,expected_flags",
#     [
#         (">",  [False, False, True]),
#         ("<",  [True, False, False]),
#         (">=", [False, True, True]),
#         ("<=", [True, True, False]),
#         ("==", [False, True, False]),
#         ("!=", [True, False, True]),
#     ],
# )
# def test_dq_compare_columns_parametrized(sample_df, operator, expected_flags):

#     result_df = dq_compare_columns(
#         sample_df,
#         column_name1="amount",
#         column_name2="threshold",
#         operator=operator,
#         flag_column_name="flag"
#     )

#     flags = [r["flag"] for r in result_df.orderBy("id").collect()]

#     assert flags == expected_flags


# def test_dq_compare_columns_invalid_operator(sample_df):

#     with pytest.raises(ValueError) as e:
#         dq_compare_columns(sample_df, "amount", "threshold", "??", "flag")

#     assert "Invalid operator" in str(e.value)

# # COMMAND ----------

# @pytest.fixture
# def df_emails(spark):
#     return spark.createDataFrame([
#         Row(id=1, email="test@example.com"),
#         Row(id=2, email="user.name+tag@domain.co"),
#         Row(id=3, email="invalid-email"),
#         Row(id=4, email="another@domain"),
#         Row(id=5, email="noatsymbol.com"),
#     ])


# @pytest.mark.parametrize(
#     "row_id,expected_flag",
#     [
#         (1, True),   # valid
#         (2, True),   # valid
#         (3, False),  # missing @ / invalid structure
#         (4, False),  # missing TLD
#         (5, False),  # missing @
#     ],
# )
# def test_dq_email_check_parametrized(df_emails, row_id, expected_flag):

#     result_df = dq_email_check(df_emails, "email", "is_valid")

#     row = result_df.filter(f"id == {row_id}").select("is_valid").first()

#     assert row["is_valid"] == expected_flag


# def test_flag_column_is_added(df_emails):

#     result_df = dq_email_check(df_emails, "email", "flag")

#     assert "flag" in result_df.columns




# # COMMAND ----------

# @pytest.mark.parametrize(
#     "df_data, df_ref_data, join_col, expected_missing",
#     [
#         (
#             # Case 1 — one missing reference
#             [{"id": 1}, {"id": 2}, {"id": 3}],
#             [{"id": 1}, {"id": 2}],
#             "id",
#             [3],
#         ),
#         (
#             # Case 2 — none missing
#             [{"id": 10}, {"id": 20}],
#             [{"id": 10}, {"id": 20}],
#             "id",
#             [],
#         ),
#         (
#             # Case 3 — all missing
#             [{"id": 5}, {"id": 6}],
#             [],
#             "id",
#             [5, 6],
#         ),
#     ],
# )
# def test_missing_reference_check(spark, df_data, df_ref_data, join_col, expected_missing):
#     df = spark.createDataFrame(df_data)
#     df_ref = spark.createDataFrame(df_ref_data)

#     result_df = missing_reference_check(df, df_ref, join_col)

#     result_ids = [row[join_col] for row in result_df.collect()]

#     assert sorted(result_ids) == sorted(expected_missing)
