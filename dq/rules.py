# Databricks notebook source
from pyspark.sql.functions import expr, col, when, count
from pyspark.sql import DataFrame, SparkSession

# COMMAND ----------

# import pandas as pd

# spark = SparkSession.builder \
#         .master("local[1]") \
#         .appName("pytest-spark") \
#         .getOrCreate()

# COMMAND ----------

def validate_columns(df: DataFrame, columns_expected_datatype: dict):
    """
    Validates the type of a column in a DataFrame.
    data type examples:
        - "string"
        - "int"
        - "double"
        - "date"

    Args:
        df: Spark DataFrame
        columns_expected_datatype: dictionary of column names and expected data types

    Returns:
        valid_df: rows where all columns match expected data type
        invalid_df: rows where any column does not match expected data type
    """

    conditions = []

    for column, expected_type in columns_expected_datatype.items():
        # Flag column
        if expected_type == "date":
            df = df.withColumn(column, expr(f"coalesce(to_date({column}, 'd/M/yyyy'), to_date({column}, 'yyyy-MM-dd'))"))
        else:
            df = df.withColumn(column, expr(f"try_cast({column} as {expected_type})"))

        conditions.append(col(column).isNotNull())

    combined_conditions = conditions[0]
    for condition in conditions[1:]:
        combined_conditions = combined_conditions & condition
        
    valid_df = df.filter(combined_conditions)
    invalid_df = df.filter(~combined_conditions)

    return valid_df, invalid_df

# COMMAND ----------

def validate_primary_key_unique(df: DataFrame, primary_key: str):
    """
    Validate primary key uniqueness and returns duplicate rows

    Args:
        df: Spark DataFrame
        primary_key: single or list of columns

    Returns:
        DataFrame containing duplicate rows
        Or message - No duplicates found
    """
    total_count = df.count()
    unique_count = df.select(primary_key).dropDuplicates().count()

    if total_count != unique_count:
        # Get duplicate keys
        df_dupliicates = df.groupBy(primary_key).count().filter("count > 1").drop("count")
        # Join to get duplicate rows
        dup_rows = df.join(df_dupliicates, primary_key)
        return dup_rows
    else:
        return "No duplicates found"

# COMMAND ----------

def null_check(df):
    """
    Checks for null values in a DataFrame
    and returns a DataFrame with the count of null values
    for columns with atleast one null value.

    Args:
        df: Spark DataFrame

    Returns:
        DataFrame with count of null values for columns with atleast one null value
    """

    # Get count of null values in each column
    df_null_count = df.select([
                            count(when(col(c).isNull(), c)).alias(c) for c in df.columns
                        ])
    
    # Convert to dict and get columns with atleast one null value
    null_count_dict = df_null_count.first().asDict()
    keep_cols = [k for k, v in null_count_dict.items() if v != 0]

    return df_null_count.select(*keep_cols)

# COMMAND ----------

def dq_compare(column_name: str, operator: str, value: str):
    """
    Checks if a column in a DataFrame satisfies a comparison condition.
    
    Args:
    df: Spark DataFrame
    column_name: column name to compare
    operator: operator to compare with

    Returns:
        Boolean
    """

    c = col(column_name)
    if operator == ">":
        return c > value
    elif operator == "<":
        return c < value
    elif operator == ">=":
        return c >= value
    elif operator == "<=":
        return c <= value
    elif operator == "==":
        return c == value
    elif operator == "!=":
        return c != value
    else:
        raise ValueError("Invalid operator. Use one of: >, <, >=, <=, ==, !=")

# COMMAND ----------

def dq_compare_columns(df: DataFrame, column_name1: str, column_name2: str, operator: str, flag_column_name: str) -> DataFrame:
    """
    Checks if a column in a DataFrame satisfies a comparison condition.

    Args:
    df: Spark DataFrame
    column_name: column name to compare
    operator: operator to compare with

    Returns:
        Dataframe
    """

    col1 = col(column_name1)
    col2 = col(column_name2)

    if operator == ">":
        expr = col1 > col2
    elif operator == "<":
        expr = col1 < col2
    elif operator == ">=":
        expr = col1 >= col2
    elif operator == "<=":
        expr = col1 <= col2
    elif operator == "==":
        expr = col1 == col2
    elif operator == "!=":
        expr = col1 != col2
    else:
        raise ValueError("Invalid operator. Use one of: >, <, >=, <=, ==, !=")

    return df.withColumn(flag_column_name, expr)

# COMMAND ----------

def dq_email_check(df: DataFrame, email_col: str, flag_column_name: str):
    """
    Checks if a column in a DataFrame contains valid email address

    Args:
    df: Spark DataFrame
    email_col: column name to check
    flag_column_name: column name to flag

    Returns:
        Dataframe
    """
    email_check = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    return df.withColumn(flag_column_name, col(email_col).rlike(email_check))

# COMMAND ----------

def missing_reference_check(df, df_ref, join_col_name):
    """
    Check if reference data is not in table yet, but it exists in transactional data.

    Args:
        df: Transactional/Fact DataFrame
        df_ref: Reference/Dimension DataFrame
        join_col_name: column name to join on
    
    Returns:
        Spark DataFrame
    """
    return df.join(df_ref, join_col_name, "left").filter(df_ref[join_col_name].isNull()).drop(df_ref[join_col_name])
