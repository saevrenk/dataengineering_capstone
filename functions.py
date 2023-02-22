import mysql.connector as db
from mapping import column_map
from pyspark.sql.functions import (
    concat,
    lpad,
    length,
    regexp_replace,
    initcap,
    lower,
    col,
    lit,
)


def get_df_size(sparkdf):
    """Return the dimensions of a spark dataframe"""
    return (sparkdf.count(), len(sparkdf.columns))


def check_database():
    # Create a database called "creditcard_capstone" if does not exist
    mysql = db.connect(user="root", password="")
    cursor = mysql.cursor()
    cursor.execute("SHOW DATABASES;")
    dbs = cursor.fetchall()
    dbs_list = [x[0] for x in dbs]
    if "creditcard_capstone" not in dbs_list:
        cursor.execute("CREATE DATABASE creditcard_capstone;")
        print("===> Created the database creditcard_capstone.")
        return 1
    else:
        print("===> Database exist, the tables will be overwritten.")
        return 0


def transform(df, data_name):
    map_dict = column_map[data_name]
    # general type casting
    for tup in map_dict:
        df = df.withColumn(tup[1], df[tup[0]].cast(tup[2]))

    # special instructions for dataframe transformations
    if data_name == "credit":
        df = df.withColumns(
            {"MONTH": lpad(df["MONTH"], 2, "0"), "DAY": lpad(df["DAY"], 2, "0")}
        )

        df = df.withColumns({"TIMEID": concat("YEAR", "MONTH", "DAY")})
        # drop columns:
        df = df.drop("YEAR", "MONTH", "DAY", "CREDIT_CARD_NO")
        df = df.select(
            "CUST_CC_NO",
            "TIMEID",
            "CUST_SSN",
            "BRANCH_CODE",
            "TRANSACTION_TYPE",
            "TRANSACTION_VALUE",
            "TRANSACTION_ID",
        )

    elif data_name == "branch":
        # check for null zip codes:
        if df.filter(df["BRANCH_ZIP"].isNull()).collect():
            df = df.fillna(value={"BRANCH_ZIP": 99999})
        # format phone_numbers:
        df = df.withColumn(
            "BRANCH_PHONE",
            regexp_replace("BRANCH_PHONE", r"(\d{3})(\d{3})(\d{4})", "($1)$2-$3"),
        )
        df = df.select(
            "BRANCH_CODE",
            "BRANCH_NAME",
            "BRANCH_STREET",
            "BRANCH_CITY",
            "BRANCH_STATE",
            "BRANCH_ZIP",
            "BRANCH_PHONE",
            "LAST_UPDATED",
        )

    elif data_name == "customer":
        df = df.withColumns(
            {
                "FIRST_NAME": initcap("FIRST_NAME"),
                "MIDDLE_NAME": lower("MIDDLE_NAME"),
                "LAST_NAME": initcap("LAST_NAME"),
            }
        )
        df = df.withColumn(
            "FULL_STREET_ADDRESS", concat(col("STREET_NAME"), lit(", "), col("APT_NO"))
        )
        df = df.withColumn(
            "CUST_PHONE", regexp_replace("CUST_PHONE", r"(\d{3})(\d{4})", "(111)$1-$2")
        )
        df = df.drop("STREET_NAME", "APT_NO")
        # reorder columns:
        df = df.select(
            "SSN",
            "FIRST_NAME",
            "MIDDLE_NAME",
            "LAST_NAME",
            "CREDIT_CARD_NO",
            "FULL_STREET_ADDRESS",
            "CUST_CITY",
            "CUST_STATE",
            "CUST_COUNTRY",
            "CUST_ZIP",
            "CUST_PHONE",
            "CUST_EMAIL",
            "LAST_UPDATED",
        )
    return df
