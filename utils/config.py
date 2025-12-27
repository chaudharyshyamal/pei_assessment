# Databricks notebook source
# Unity Catalog settings
catalog = "pei_assessment_catalog"
volumes_schema = "volumes_schema"
raw_schema = "raw_schema"

# Volume paths
volumes = {
    "orders_path": "/Volumes/pei_assessment_catalog/volumes_schema/pei_assessment_datasets/Orders.json",
    "products_path": "/Volumes/pei_assessment_catalog/volumes_schema/pei_assessment_datasets/Products.csv",
    "customers_path": "/Volumes/pei_assessment_catalog/volumes_schema/pei_assessment_datasets/Customer.xlsx"
}

# Raw Table names
raw_tables = {
    "customers": f"{catalog}.{raw_schema}.customers",
    "products": f"{catalog}.{raw_schema}.products",
    "orders": f"{catalog}.{raw_schema}.orders"
}

dq_rules = {
    "products_rules": {"price_per_product": "double"},
    "orders_rules": {"discount": "double",
                    "order_date": "date",
                    "price": "double",
                    "profit": "double",
                    "quantity": "long",
                    "row_id": "long",
                    "ship_date": "date"},
    "customers_rules": {"postal_code" : "long"}
}
