#!/usr/bin/env python

import os
import re
import mysql.connector
import pyinputplus as pyip
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, regexp_extract, col
from functions import (
    transaction_by_customer,
    get_val_trans_type,
    get_trans_state,
    get_cust_info,
    modify_cust_record,
    mysql_updater,
    generate_bill,
    get_trans_interval,
)

mysql_pwd = os.environ.get("mysql_root_p")

# Start SPARK session:
spark = (
    SparkSession.builder.appName("capstone_analysis")
    .config(
        "spark.jars",
        "/opt/homebrew/Cellar/apache-spark/3.3.1/libexec/jars/mysql-connector-j-8.0.32.jar",
    )
    .getOrCreate()
)

# READ the tables from the database and create temp views:
credit = spark.read.jdbc(
    "jdbc:mysql://localhost:3306/creditcard_capstone",
    "CDW_SAPP_CREDIT_CARD",
    properties={
        "user": "root",
        "password": mysql_pwd,
        "driver": "com.mysql.cj.jdbc.Driver",
    },
)

branch = spark.read.jdbc(
    "jdbc:mysql://localhost:3306/creditcard_capstone",
    "CDW_SAPP_BRANCH",
    properties={
        "user": "root",
        "password": mysql_pwd,
        "driver": "com.mysql.cj.jdbc.Driver",
    },
)

customer = spark.read.jdbc(
    "jdbc:mysql://localhost:3306/creditcard_capstone",
    "CDW_SAPP_CUSTOMER",
    properties={
        "user": "root",
        "password": mysql_pwd,
        "driver": "com.mysql.cj.jdbc.Driver",
    },
)

credit.createTempView("credit_tb")
branch.createTempView("branch_tb")
customer.createTempView("customer_tb")


transaction_options = [
    "Display the transactions by customers living in a given zip code for a given month and year (Ordered by day descending)",
    "Display the number and total values of transactions for a given transaction type",
    "Display the total number and total values of transactions for branches in a given state",
    "Return to main menu",
    "Exit",
]

cust_options = [
    "Check the existing account details of a customer",
    "Modify the existing account details of a customer",
    "Generate a monthly bill for a credit card number for a given month and year",
    "Display the transactions made by a customer between two dates. (Order by year, month, and day in descending order)",
    "Return to main menu",
    "Exit",
]


main_menu = True

while main_menu:
    menu = pyip.inputMenu(
        ["Transaction Details Module", "Customer Details Module", "Exit"], numbered=True
    )
    if menu == "Transaction Details Module":
        sub_menu = True
        while sub_menu:
            submenu = pyip.inputMenu(transaction_options, numbered=True)
            if submenu == transaction_options[0]:
                query = transaction_by_customer()
                spark.sql(query).show(30)
            elif submenu == transaction_options[1]:
                query = get_val_trans_type()
                spark.sql(query).show(30)
            elif submenu == transaction_options[2]:
                query = get_trans_state()
                spark.sql(query).show()
            elif submenu == transaction_options[3]:
                sub_menu = False
            elif submenu == transaction_options[4]:
                sub_menu = False
                main_menu = False

    elif menu == "Customer Details Module":
        sub_menu = True
        while sub_menu:
            submenu = pyip.inputMenu(cust_options, numbered=True)
            if submenu == cust_options[0]:
                query, columns = get_cust_info()
                dfnew = spark.sql(query)
                # mask SSN
                dfnew = dfnew.withColumn(
                    "SSN_masked",
                    regexp_replace(dfnew["SSN"], r"(\d{5})(\d{4})", "*****$2"),
                )
                dfnew.select(columns).show()
            elif submenu == cust_options[1]:
                ssn, changes = modify_cust_record()
                mysql_updater(changes)
            elif submenu == cust_options[2]:
                query = generate_bill()
                spark.sql(query).show()
            elif submenu == cust_options[3]:
                query = get_trans_interval()
                spark.sql(query).show()
            elif submenu == cust_options[4]:
                sub_menu = False
            elif submenu == cust_options[5]:
                sub_menu = False
                main_menu = False
