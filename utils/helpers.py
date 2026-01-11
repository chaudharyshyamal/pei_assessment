# Databricks notebook source
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, desc, asc, round

# COMMAND ----------

import pandas as pd

# spark = SparkSession.builder \
#         .master("local[1]") \
#         .appName("pytest-spark") \
#         .getOrCreate()

# COMMAND ----------

def read_volume_files(spark: SparkSession, volume_path: str, column_mapping_dict: dict = {}, **options) -> DataFrame:
    """
    Reads JSON, CSV, XLSX as Spark DataFrame from Volumes,
    cast column types using schema and mapping dictionary to convert all column names to snake case.

    Args:
        volume_path (str): Path to the volume.
        schema (StructType): Schema for the DataFrame.
        column_mapping_dict (dict): Dictionary for column name mapping.
        **options: Keyword arguments for Spark DataFrame options.
    
    Returns:
        Spark DataFrame
    """

    default_options = {"header": "true",
                       "inferSchema": "true"
                    }
    default_options.update(options)

    if volume_path.endswith(".csv"):
        df = spark.read.format("csv").options(**default_options).load(volume_path)
    elif volume_path.endswith(".json"):
        df = spark.read.format("json").options(**default_options).load(volume_path)
    elif volume_path.endswith(".xlsx"):
        # df = spark.read.options(**default_options).excel(volume_path)
        df = spark.read.format("excel").options(**default_options).load(volume_path)
    else:
        raise ValueError(f"Unsupported file format: {volume_path}")

    df = df.withColumnsRenamed(column_mapping_dict)

    return df

# COMMAND ----------

def cast_columns(df: DataFrame, schema: None):
    """
    Cast columns as per the schemna

    Args:
        df (DataFrame): Input DataFrame.
        schema (StructType): Schema for the DataFrame.

    Returns:
        DataFrame
    """
    return df.select(*[col(f.name).cast(f.dataType).alias(f.name) for f in schema])

# COMMAND ----------

def raw_date_write(df, full_table_name):
    df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)

# COMMAND ----------

def aggregate_calculation(df, groupby_cols, agg_col, agg_func, round_upto, alias_col_name, order_cols):
    """
    Get aggregate calculation for given columns and aggregate function and order columns

    Args:
        df (DataFrame): Input DataFrame.
        groupby_cols (list): List of columns to group by.
        agg_col (str): Column to aggregate.
        agg_func (str): Aggregate function.
        round_upto (int): Number of decimal places to round to.
        alias_col_name (str): Alias for the aggregated column.
        order_cols (list): List of columns to order by.
        
    Returns:
        DataFrame: Aggregated DataFrame with the specified columns and aggregate function.
    """
    exprs = [
        desc(col) if order.lower() == "desc" else asc(col)
        for col, order in order_cols
    ]

    return df.groupBy(groupby_cols) \
            .agg(round(agg_func(agg_col), round_upto).alias(alias_col_name)) \
            .orderBy(*exprs)
