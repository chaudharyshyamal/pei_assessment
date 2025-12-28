# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.functions import to_date, col, round, year, coalesce, lit, sum, date_diff, datediff, countDistinct, avg, min, max, desc, asc, expr, count, when

# COMMAND ----------

# MAGIC %run ../utils/config

# COMMAND ----------

# MAGIC %run ../utils/schemas

# COMMAND ----------

# MAGIC %run ../utils/helpers

# COMMAND ----------

# MAGIC %run ../dq/rules

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Create raw tables for each source dataset
# MAGIC - Read as raw tables
# MAGIC - Rename columns to snake case
# MAGIC - Using data quality, either exclude or quarantine invalid records
# MAGIC - Cast to defined data types

# COMMAND ----------

# MAGIC %md
# MAGIC ### Products

# COMMAND ----------

# Read Products data from volumes
df_products = read_volume_files(volumes["products_path"], products_mapping, sep = ",")

# Data Quality, validate columns - values and data types
valid_products_df, invalid_products_df = validate_columns(df_products, dq_rules["products_rules"])

# Cast columns using defined schema
df_products = cast_columns(valid_products_df, products_schema)

# Data Quality, compare column values
compare_df = df_products.withColumn("dq_pass", dq_compare("price_per_product", ">=", "0"))
# Products data after DQ - compare values and filter
df_products = compare_df.filter(col("dq_pass") == True).drop("dq_pass")
#  Products data after DQ - compare values and invalid data
invalid_compare_df = compare_df.filter(col("dq_pass") == False)

# Duplicate check on product_id (assuming Primary key)
# Not dropping duplicates as product name, state and price is different
# Based on business requirement or some timestamp (implement SCD), then one can decide
duplicate_products_rows_df = validate_primary_key_unique(df_products, ["product_id"])

# Null check, will give null counts in columns with atleast one null value
null_counts_df = null_check(df_products)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Orders

# COMMAND ----------

# Read Orders data from volumes
df_orders = read_volume_files(volumes["orders_path"], orders_mapping, multiline="true")

# Data Quality, validate columns - values and data types
valid_orders_df, invalid_orders_df = validate_columns(df_orders, dq_rules["orders_rules"])

# Cast columns using defined schema
df_orders = cast_columns(valid_orders_df, orders_schema)

# Data Quality, compare column values, price should be greater than equal to zero
compare_price_df = df_orders.withColumn("dq_pass", dq_compare("price", ">=", "0"))
# Orders data after DQ - compare values and filter
df_orders = compare_price_df.filter(col("dq_pass") == True).drop("dq_pass")
#  Orders data after DQ - compare values and invalid data
invalid_price_df = compare_price_df.filter(col("dq_pass") == False)

# Data Quality, compare column values, quantity should be greater than equal to zero
compare_quantity_df = df_orders.withColumn("dq_pass", dq_compare("quantity", ">=", "0"))
# Orders data after DQ - compare values and filter
df_orders = compare_quantity_df.filter(col("dq_pass") == True).drop("dq_pass")
#  Orders data after DQ - compare values and invalid data
invalid_quantity_df = compare_quantity_df.filter(col("dq_pass") == False)

# Duplicate check on order_id and product_id (assuming Primary key)
# Not dropping duplicates as price, profit and row_id is different
# Based on business requirement or some timestamp (implement SCD), then one can decide
duplicate_orders_rows_df = validate_primary_key_unique(df_orders, ["order_id", "product_id"])

# Data Quality, check if ship_date is greater than order_date
# If not, then put it as quarantine records
df_orders = dq_compare_columns(df_orders, "ship_date", "order_date", ">=", "dq_pass").filter(col("dq_pass") == True).drop("dq_pass")
invalid_compare_cols_df = dq_compare_columns(df_orders, "ship_date", "order_date", ">=", "dq_pass").filter(col("dq_pass") == False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers

# COMMAND ----------

# Read Customers data from volumes
df_customers = read_volume_files(volumes["customers_path"], customers_mapping, sheetName = "Worksheet", headerRows = 1)

# Data Quality, validate columns - values and data types
valid_customers_df, invalid_customers_df = validate_columns(df_customers, dq_rules["customers_rules"])

# Cast columns using defined schema
df_customers = cast_columns(valid_customers_df, customers_schema)

# Data Quality, compare column values, postal code should be greater than zero
compare_postal_code_df = df_customers.withColumn("dq_pass", dq_compare("postal_code", ">", "0"))
# Customers data after DQ - compare values and filter
df_customers = compare_postal_code_df.filter(col("dq_pass") == True).drop("dq_pass")
#  Customers data after DQ - compare values and invalid data
invalid_postal_code_df = compare_postal_code_df.filter(col("dq_pass") == False)

# Duplicate check on customer (assuming Primary key)
# Not dropping duplicates as price, profit and row_id is different
# Based on business requirement or some timestamp (implement SCD), then one can decide
duplicate_customers_rows_df = validate_primary_key_unique(df_customers, ["customer_id"])

# Data Quality, check if email is valid
df_customers = dq_email_check(df_customers, "email", "dq_pass").filter(col("dq_pass") == True)
invalid_email_df = dq_email_check(df_customers, "email", "dq_pass").filter(col("dq_pass") == False)

# COMMAND ----------

# Write raw data to raw tables
raw_date_write(df_products, raw_tables["products"])
raw_date_write(df_orders, raw_tables["orders"])
raw_date_write(df_customers, raw_tables["customers"])

# COMMAND ----------

# Data Quality - check for missing product_id in products table, but present in orders table
missing_products_df = missing_reference_check(df_orders, df_products, "product_id")
if missing_products_df.count() > 0:
    print("Missing product_id in products table, but present in orders table")
else:
    print("No missing product_id in products table, but present in orders table")

# COMMAND ----------

# Data Quality - check for missing customer_id in customers table, but present in orders table
missing_customers_df = missing_reference_check(df_orders, df_customers, "customer_id")
if missing_customers_df.count() > 0:
    print("Missing product_id in products table, but present in orders table")
else:
    print("No missing product_id in products table, but present in orders table")

# COMMAND ----------

def missing_reference(df, df_ref, join_col_name):
    return df.join(df_ref, join_col_name, "left").drop(df_ref[join_col_name]).filter(col(join_col_name).isNull())

missing_reference_df = missing_reference(df_orders, df_products, "product_id")
if missing_reference_df.count() > 0:
    print("Missing product_id in products table, but present in orders table")
else:
    print("No missing product_id in products table, but present in orders table")

# COMMAND ----------

# display(df_orders.join(df_products, "product_id", "left").drop(df_products.product_id).filter(col("product_id").isNull()))

# COMMAND ----------

# display(df_orders.join(df_customers, "customer_id", "left").drop(df_customers.customer_id).filter(col("customer_id").isNull()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Create an enriched table for customers and products

# COMMAND ----------

# Join Orders, Customers and Products tables
df_orders_customers_products = df_orders.join(df_customers, \
                                   "customer_id", \
                                   "left") \
                                   .drop(df_customers.customer_id) \
                                   .withColumnRenamed("state", "customer_state") \
                                   .withColumn("order_year", year(col("order_date"))) \
                                   .join(df_products, \
                                        "product_id", \
                                        "left") \
                                        .drop(df_products.product_id) \
                                        .withColumnRenamed("state", "product_state")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Orders enrichment

# COMMAND ----------

df_enriched_orders = df_orders_customers_products.withColumn("total_sales", round(col("price") * col("quantity"), 2)) \
                                                .withColumn("discount_amount", round(col("price") * col("discount"), 2)) \
                                                .withColumn("final_price", round(col("total_sales") - col("discount_amount"), 2)) \
                                                .withColumn("unit_selling_price", round(col("final_price") / col("quantity"), 2)) \
                                                .withColumn("days_to_ship", datediff("ship_date", "order_date"))

# display(df_enriched_orders)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers enrichment

# COMMAND ----------

df_enriched_customers = df_enriched_orders.groupBy("customer_id") \
                                              .agg(
                                                    sum("final_price").alias("customer_lifetime_value"), \
                                                    sum("profit").alias("customer_lifetime_profit"), \
                                                    countDistinct("order_id").alias("total_orders"), \
                                                    avg("quantity").alias("avg_bag_size"), \
                                                    avg("final_price").alias("avg_order_value"), \
                                                    min("order_date").alias("first_order_date"), \
                                                    max("order_date").alias("latest_order_date")
                                              )

# display(df_enriched_customers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Products enrichment

# COMMAND ----------

df_enriched_products = df_enriched_orders.groupBy("product_id") \
                                            .agg(
                                                  sum("quantity").alias("total_units_sold"), \
                                                  sum("final_price").alias("total_revenue_generated"), \
                                                  avg("discount_amount").alias("avg_discount"), \
                                                  countDistinct("customer_id").alias("distinct_customers_bought")
                                            )

# display(df_enriched_products)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final enriched table

# COMMAND ----------

df_enriched_2 = df_enriched_orders.join(df_enriched_products, "product_id", "left") \
                                  .join(df_enriched_customers, "customer_id", "left")

# display(df_enriched_2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Create an enriched table which has
# MAGIC - order information 
# MAGIC - Profit rounded to 2 decimal places
# MAGIC - Customer name and country
# MAGIC - Product category and sub category
# MAGIC

# COMMAND ----------

df_enriched_3 = df_orders_customers_products.select("order_id",
                                                "customer_name",
                                                "country",
                                                "category",
                                                "sub_category",
                                                "quantity",
                                                "price",
                                                "discount",
                                                round(col("profit"), 2).alias("profit"))
# display(df_enriched_3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) Create an aggregate table that shows profit by 
# MAGIC - Year
# MAGIC - Product Category
# MAGIC - Product Sub Category
# MAGIC - Customer

# COMMAND ----------

df_enriched_4 = df_orders_customers_products.groupBy(
                                            col("order_year"),
                                            col("category"),
                                            col("sub_category"),
                                            col("customer_name")  
                                        ) \
                                        .agg(round(sum("profit").alias("total_profit"), 2)) \
                                        .orderBy("order_year", "category", "sub_category", "customer_name")

# display(df_enriched_4)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5) Using SQL output the following aggregates
# MAGIC - Profit by Year
# MAGIC - Profit by Year + Product Category
# MAGIC - Profit by Customer
# MAGIC - Profit by Customer + Year

# COMMAND ----------

df_orders_customers_products.createOrReplaceTempView("orders_customers_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Profit by Year

# COMMAND ----------

# MAGIC %md
# MAGIC SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     order_year,
# MAGIC     ROUND(SUM(profit), 2) as profit_per_year
# MAGIC FROM orders_customers_products
# MAGIC GROUP BY order_year
# MAGIC ORDER BY order_year;

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark

# COMMAND ----------

df_profit_by_year = df_orders_customers_products.groupBy("order_year") \
                                                .agg(round(sum("profit"), 2).alias("profit_per_year")) \
                                                .orderBy("order_year")

# display(df_profit_by_year)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Profit by Year + Product Category

# COMMAND ----------

# MAGIC %md
# MAGIC SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     order_year,
# MAGIC     category,
# MAGIC     ROUND(SUM(profit), 2) as profit_per_year_category
# MAGIC FROM orders_customers_products
# MAGIC GROUP BY order_year, category
# MAGIC ORDER BY order_year ASC, profit_per_year_category DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark

# COMMAND ----------

df_profit_by_year_category = df_orders_customers_products.groupBy("order_year", "category") \
                                                        .agg(round(sum("profit"), 2).alias("profit_per_year_category")) \
                                                        .orderBy(asc("order_year"), desc("profit_per_year_category"))
# display(df_profit_by_year_category)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Profit by Customer

# COMMAND ----------

# MAGIC %md
# MAGIC SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_name,
# MAGIC     ROUND(SUM(profit), 2) as profit_per_customer
# MAGIC FROM orders_customers_products
# MAGIC GROUP BY customer_name
# MAGIC ORDER BY profit_per_customer DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark

# COMMAND ----------

df_profit_by_customer = df_orders_customers_products.groupBy("customer_name") \
                                                    .agg(round(sum("profit"), 2).alias("profit_per_customer")) \
                                                    .orderBy(desc("profit_per_customer"))
# display(df_profit_by_customer)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Profit by Customer + Year

# COMMAND ----------

# MAGIC %md
# MAGIC SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_name,
# MAGIC     order_year,
# MAGIC     ROUND(SUM(profit), 2) as profit_per_customer_year
# MAGIC FROM orders_customers_products
# MAGIC GROUP BY customer_name, order_year 
# MAGIC ORDER BY order_year ASC, profit_per_customer_year DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark

# COMMAND ----------

df_profit_by_customer_year = df_orders_customers_products.groupBy("customer_name", "order_year") \
                                                        .agg(round(sum("profit"), 2).alias("profit_per_customer_year")) \
                                                        .orderBy(asc("order_year"), desc("profit_per_customer_year"))
# display(df_profit_by_customer_year)
