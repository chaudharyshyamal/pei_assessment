# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

# COMMAND ----------

def read_volume_files(volume_path: str, column_mapping_dict: dict = {}, **options) -> DataFrame:
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
        df = spark.read.options(**default_options).excel(volume_path)
    else:
        raise ValueError(f"Unsupported file format: {volume_path}")

    df = df.withColumnsRenamed(column_mapping_dict)

    return df

# COMMAND ----------

def cast_columns(df: DataFrame, schema: None):
    return df.select(*[col(f.name).cast(f.dataType).alias(f.name) for f in schema])

# COMMAND ----------

def raw_date_write(df, full_table_name):
    df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)
