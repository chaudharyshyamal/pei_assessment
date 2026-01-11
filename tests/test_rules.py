import pytest
from pyspark.sql import Row
from dq.rules import validate_columns, validate_primary_key_unique, null_check, dq_compare, dq_compare_columns, dq_email_check, missing_reference_check


# Unit test - validate_columns
@pytest.fixture
def test_df(spark):
    data = [
        Row(id=1, name="Alice", age="25", salary="1000.5", join_date="01/01/2023"),
        Row(id=2, name="Bob", age="thirty", salary="2000", join_date="2023-02-15"),
        Row(id=3, name="Charlie", age="40", salary="abc", join_date="15/03/2023"),
        Row(id=4, name=None, age="35", salary="1500", join_date="2023-13-01"),
    ]

    return spark.createDataFrame(data)


@pytest.mark.parametrize(
    "columns_expected_datatype,expected_valid_ids,expected_invalid_ids",
    [
        (
            {"id": "int", "age": "int"},  # check integer casting
            [1, 3, 4],  # valid rows where id and age cast succeed
            [2]         # invalid rows (age="thirty")
        ),
        (
            {"salary": "double"},         # check double casting
            [1, 2, 4],  # salary is valid double
            [3]         # salary="abc" invalid
        ),
        (
            {"join_date": "date"},        # check date parsing
            [1,2,3],    # valid date formats
            [4]         # invalid date
        ),
        (
            {"name": "string", "age": "int"},  # mixed types
            [1,3],    # valid rows
            [2,4]         # invalid row
        )
    ]
)
def test_validate_columns(test_df, columns_expected_datatype, expected_valid_ids, expected_invalid_ids):
    valid_df, invalid_df = validate_columns(test_df, columns_expected_datatype)

    valid_ids = sorted([row.id for row in valid_df.collect()])
    assert valid_ids == expected_valid_ids

    invalid_ids = sorted([row.id for row in invalid_df.collect()])
    assert invalid_ids == expected_invalid_ids

    assert valid_df.count() + invalid_df.count() == test_df.count()



# Unit test - validate_primary_key_unique
@pytest.fixture
def df_with_duplicates(spark):
    data = [
        Row(id=1, name="Alice"),
        Row(id=2, name="Bob"),
        Row(id=1, name="Alice Dup"),
        Row(id=3, name="Charlie"),
        Row(id=2, name="Bob Dup"),
    ]
    return spark.createDataFrame(data)

@pytest.fixture
def df_no_duplicates(spark):
    data = [
        Row(id=1, name="Alice"),
        Row(id=2, name="Bob"),
        Row(id=3, name="Charlie"),
    ]
    return spark.createDataFrame(data)


@pytest.mark.parametrize(
    "df_fixture,expected_ids,expect_message",
    [
        ("df_with_duplicates", [1,2], False),  # duplicates exist
        ("df_no_duplicates", [], True),        # no duplicates
    ]
)
def test_validate_primary_key_unique(df_fixture, request, expected_ids, expect_message):
    df = request.getfixturevalue(df_fixture)

    result = validate_primary_key_unique(df, "id")

    if expect_message:
        assert result == "No duplicates found"
    else:
        # Should return a DataFrame with duplicate rows
        ids_in_result = sorted([row.id for row in result.collect()])
        for key in expected_ids:
            assert key in ids_in_result

        assert result.count() > len(expected_ids)



# Unit test - null_check
@pytest.fixture
def null_df(spark):
    data = [
        Row(id=1, name="Alice", age=25),
        Row(id=2, name=None, age=30),
        Row(id=3, name="Bob", age=None),
        Row(id=4, name=None, age=None),
    ]
    return spark.createDataFrame(data)


@pytest.mark.parametrize(
    "expected_counts",
    [
        ({"name": 2, "age": 2}),   # Expected null counts in columns with nulls
    ]
)
def test_null_check(null_df, expected_counts):
    result_df = null_check(null_df)

    result_row = result_df.collect()[0]
    result = result_row.asDict()

    # Only columns with nulls should appear
    assert set(result.keys()) == set(expected_counts.keys())

    # Validate null counts
    for col_name, count_expected in expected_counts.items():
        assert result[col_name] == count_expected

    assert result_df.count() == 1



# Unit test - dq_compare
@pytest.fixture
def sample_df(spark):
    data = [
        Row(id=1, score=10),
        Row(id=2, score=5),
        Row(id=3, score=7),
    ]
    return spark.createDataFrame(data)


@pytest.mark.parametrize(
    "operator,value,expected",
    [
        (">", 6,  {1: True, 2: False, 3: True}),
        ("<", 6,  {1: False, 2: True, 3: False}),
        (">=", 7, {1: True, 2: False, 3: True}),
        ("<=", 7, {1: False, 2: True, 3: True}),
        ("==", 7, {1: False, 2: False, 3: True}),
        ("!=", 7, {1: True, 2: True, 3: False}),
    ]
)
def test_dq_compare_column(sample_df, operator, value, expected):
    result_df = sample_df.withColumn("flag", dq_compare("score", operator, value))

    assert "flag" in result_df.columns

    result = {row.id: row.flag for row in result_df.collect()}
    assert result == expected

    assert result_df.count() == sample_df.count()


def test_dq_compare_invalid_operator(sample_df):
    with pytest.raises(ValueError):
        dq_compare("score", "INVALID", 10)



# Unit test - dq_compare_columns
@pytest.fixture
def compare_df(spark):
    data = [
        Row(id=1, a=10, b=5),
        Row(id=2, a=5,  b=10),
        Row(id=3, a=7,  b=7),
    ]
    return spark.createDataFrame(data)

@pytest.mark.parametrize(
    "operator,expected",
    [
        (">",  {1: True,  2: False, 3: False}),
        ("<",  {1: False, 2: True,  3: False}),
        (">=", {1: True,  2: False, 3: True}),
        ("<=", {1: False, 2: True,  3: True}),
        ("==", {1: False, 2: False, 3: True}),
        ("!=", {1: True,  2: True,  3: False}),
    ]
)
def test_dq_compare_columns(compare_df, operator, expected):
    result_df = dq_compare_columns(
        compare_df,
        "a",
        "b",
        operator,
        "compare_flag"
    )

    assert "compare_flag" in result_df.columns

    result = {row.id: row.compare_flag for row in result_df.collect()}
    assert result == expected

    assert result_df.count() == compare_df.count()


def test_dq_compare_columns_invalid_operator(compare_df):
    with pytest.raises(ValueError):
        dq_compare_columns(compare_df, "a", "b", "INVALID", "compare_flag")



# Unit test - dq_email_check
@pytest.fixture
def email_df(spark):
    data = [
        Row(id=1, email="test.user@example.com"),   # valid
        Row(id=2, email="user123@gmail.co.in"),     # valid
        Row(id=3, email="invalid-email"),           # invalid
        Row(id=4, email="user@com"),                # invalid
        Row(id=5, email="@gmail.com"),              # invalid
        Row(id=6, email=None),                      # null
    ]
    return spark.createDataFrame(data)


@pytest.mark.parametrize(
    "test_id,expected_flag",
    [
        (1, True),
        (2, True),
        (3, False),
        (4, False),
        (5, False),
        (6, None),
    ]
)
def test_dq_email_check(email_df, test_id, expected_flag):
    result_df = dq_email_check(email_df, "email", "is_valid_email")

    assert "is_valid_email" in result_df.columns

    row = result_df.filter(f"id = {test_id}").collect()[0]
    assert row.is_valid_email == expected_flag

    assert result_df.count() == email_df.count()



# Unit test - missing_reference_check
@pytest.fixture
def txn_data(spark):
    data = [
        ("1", "10.5"),
        ("2", "20.0"),
        ("3", "30.0"),
    ]
    return spark.createDataFrame(
        data,
        ["id", "amount"]
    )


@pytest.fixture
def ref_data(spark):
    data = [
        ("1", "abc"),
        ("2", "xyz"),
    ]
    return spark.createDataFrame(
        data,
        ["id", "name"]
    )

def test_missing_reference_check(txn_data, ref_data):
    """
    Validate that rows present in txn_data but missing in ref_data are returned
    """

    result_df = missing_reference_check(
        df=txn_data,
        df_ref=ref_data,
        join_col_name="id"
    )

    result = result_df.collect()

    assert len(result) == 1
    assert result[0]["id"] == "3"
    assert result[0]["amount"] == "30.0"