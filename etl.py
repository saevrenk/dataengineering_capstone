#!/usr/bin/env python

import os
import requests
import configparser
import pandas as pd
from functions import get_df_size, transform, check_database
from mapping import table_names, column_types_mysql

from pyspark.sql import SparkSession

# parse config.ini
config = configparser.ConfigParser()
config.read(os.path.dirname(__file__) + "/config.ini")

# read data sources from config.ini
credit_source = config.get("CREDIT", "data")
branch_source = config.get("BRANCH", "data")
customer_source = config.get("CUSTOMER", "data")
loan_source = config.get("LOAN", "url")

spark = (
    SparkSession.builder.appName("capstone_etl")
    .config(
        "spark.jars",
        config.get("JARS", "connector"),
    )
    .getOrCreate()
)

# get password for DB connection
mysql_pwd = os.environ.get("mysql_root_p")

# EXTRACT

# Read JSON files
print("===> Reading JSON files")
branch = spark.read.json(branch_source)
credit = spark.read.json(credit_source)
customer = spark.read.json(customer_source)

# Loan application API
print("===> Reading loan data from rest API")
url = loan_source
try:
    response = requests.get(url)
except requests.exceptions.RequestException as e:
    raise SystemExit(e)
print(f"===> Status code for the API connection is {response}")
pandas_df = pd.DataFrame(response.json())
loan = spark.createDataFrame(pandas_df)

assert get_df_size(credit) == config.get(
    "CREDIT", "size"
), "credit dataframe reading failed"
assert get_df_size(branch) == config.get(
    "BRANCH", "size"
), "branch dataframe reading failed"
assert get_df_size(customer) == config.get(
    "CUSTOMER", "size"
), "customer dataframe reading failed"
assert get_df_size(loan) == config.get("LOAN", "size"), "loan dataframe reading failed"

print("===> Finished data extraction successfully.")

# TRANSFORM
print("===> Transforming dataframes according to mapping specifications.")
customer_transformed = transform(customer, "customer")
credit_transformed = transform(credit, "credit")
branch_transformed = transform(branch, "branch")
loan_transformed = loan

# Create a database called "creditcard_capstone: if does not exist
check_database()

# LOAD
print("===> Loading the dataframes to the creditcard_capstone database.")

for dfname, tab_name in table_names.items():
    eval(
        f'{dfname}.write.format("jdbc")\
        .mode("overwrite")\
        .option( "url", "jdbc:mysql://localhost:3306/creditcard_capstone")\
        .option("dbtable", "creditcard_capstone.{tab_name}")\
        .option("user", "root")\
        .option("password", "{mysql_pwd}")\
        .option("driver", "com.mysql.cj.jdbc.Driver")\
        .option("createTableColumnTypes", "{column_types_mysql[dfname]}").save()'
    )
    print(f"===> Uploaded {dfname} to {tab_name}")

print("===> Finished ETL successfully.")
