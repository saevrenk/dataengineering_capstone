#!/usr/bin/env python

import os
import requests
import pandas as pd
from functions import get_df_size, transform, check_database
from mapping import table_names, column_types_mysql

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("capstone_etl")
    .config(
        "spark.jars",
        "/opt/homebrew/Cellar/apache-spark/3.3.1/libexec/jars/mysql-connector-j-8.0.32.jar",
    )
    .getOrCreate()
)
# authentication for DB connection
mysql_pwd = os.environ.get("mysql_root_p")

# EXTRACT

# Read JSON files
print("===> Reading JSON files")
branch = spark.read.json("./Credit Card Dataset/cdw_sapp_branch.json")
credit = spark.read.json("./Credit Card Dataset/cdw_sapp_credit.json")
customer = spark.read.json("./Credit Card Dataset/cdw_sapp_custmer.json")

# Loan application API
print("===> Reading loan data from rest API")
url = "https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json"
try:
    response = requests.get(url)
except requests.exceptions.RequestException as e:
    raise SystemExit(e)
print(f"===> Status code for the API connection is {response}")
pandas_df = pd.DataFrame(response.json())
loan = spark.createDataFrame(pandas_df)

assert get_df_size(credit) == (46694, 9), "credit dataframe failed"
assert get_df_size(branch) == (115, 8), "branch dataframe failed"
assert get_df_size(customer) == (952, 14), "customer dataframe failed"
assert get_df_size(loan) == (511, 10), "loan dataframe failed"
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
