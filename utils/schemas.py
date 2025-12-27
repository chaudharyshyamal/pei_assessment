# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType, DateType

# COMMAND ----------

products_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("sub_category", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("state", StringType(), True),
    StructField("price_per_product", DoubleType(), True)
])

products_mapping = {
    "Product ID": "product_id",
    "Category": "category",
    "Sub-Category": "sub_category",
    "Product Name": "product_name",
    "State": "state",
    "Price per product": "price_per_product"
}

# COMMAND ----------

orders_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("discount", DoubleType(), True),
    StructField("order_date", DateType(), True),
    StructField("order_id", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("product_id", StringType(), True),
    StructField("profit", DoubleType(), True),
    StructField("quantity", LongType(), True),
    StructField("row_id", LongType(), True),
    StructField("ship_date", DateType(), True),
    StructField("ship_mode", StringType(), True)
])

orders_mapping = {
    "Customer ID": "customer_id",
    "Discount": "discount",
    "Order Date": "order_date",
    "Order ID": "order_id",
    "Price": "price",
    "Product ID": "product_id",
    "Profit": "profit",
    "Quantity": "quantity",
    "Row ID": "row_id",
    "Ship Date": "ship_date",
    "Ship Mode": "ship_mode"
}

# COMMAND ----------

customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("postal_code", LongType(), True),
    StructField("region", StringType(), True)
])

customers_mapping = {
    "Customer ID": "customer_id",
    "Customer Name": "customer_name",
    "email": "email",
    "phone": "phone",
    "address": "address",
    "Segment": "segment",
    "Country": "country",
    "City": "city",
    "State": "state",
    "Postal Code": "postal_code",
    "Region": "region"
}
