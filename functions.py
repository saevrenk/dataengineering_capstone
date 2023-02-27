import os
import re
import mysql.connector as db
from mapping import column_map
import pyinputplus as pyip

from pyspark.sql.functions import (
    concat,
    lpad,
    regexp_replace,
    initcap,
    lower,
    col,
    lit,
)

# authentication for DB connection:
mysql_pwd = os.environ.get("mysql_root_p")


def get_df_size(sparkdf):
    """Return the dimensions of a spark dataframe"""
    return (sparkdf.count(), len(sparkdf.columns))


def check_database():
    """Create a database called "creditcard_capstone" if does not exist"""
    mysql = db.connect(user="root", password=mysql_pwd)
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
    """Transform data according to the mapping.column_map specifications"""
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


def transaction_by_customer():
    zipcode = pyip.inputInt("Enter 4 or 5 digit zipcode: ", min=1000, max=99999)
    month = pyip.inputInt("Enter month as an integer (1-12): ", min=1, max=12)
    year = pyip.inputInt("Enter year as an integer (1900-2023): ", min=1900, max=2023)
    year = str(year)
    month = str(month)
    date_string = year + month.zfill(2)
    q = (
        "SELECT credit_tb.TRANSACTION_TYPE, "
        "credit_tb.TRANSACTION_VALUE, "
        "credit_tb.TRANSACTION_ID, "
        "credit_tb.TIMEID, "
        "customer_tb.CUST_ZIP "
        "FROM credit_tb "
        "JOIN customer_tb "
        "ON credit_tb.CUST_SSN = customer_tb.SSN "
        f"WHERE customer_tb.CUST_ZIP = {zipcode} "
        f"AND credit_tb.TIMEID LIKE '{date_string}%' "
        "ORDER BY TIMEID DESC;"
    )
    return q


def get_val_trans_type():
    transaction_type = pyip.inputMenu(
        ["Education", "Entertainment", "Healthcare", "Grocery", "Test", "Gas", "Bills"],
        numbered=True,
    )
    q = (
        f"SELECT TRANSACTION_TYPE, ROUND(SUM(TRANSACTION_VALUE),2) AS Total_USD "
        "FROM credit_tb "
        "GROUP BY TRANSACTION_TYPE "
        f"HAVING TRANSACTION_TYPE = '{transaction_type}';"
    )
    return q


def get_trans_state():
    state = pyip.inputRegex(
        r"^[A-Za-z]{2}$", prompt="Enter a US state abbreviation: "
    ).upper()

    q = (
        "SELECT branch_tb.BRANCH_STATE, "
        "COUNT(TRANSACTION_ID) AS Number_of_Transactions, "
        "ROUND(SUM(TRANSACTION_VALUE),2) AS Total_USD "
        "from credit_tb JOIN branch_tb ON credit_tb.BRANCH_CODE = branch_tb.BRANCH_CODE "
        f"WHERE branch_tb.BRANCH_STATE = '{state}' "
        "GROUP BY branch_tb.BRANCH_STATE;"
    )
    return q


def get_cust_info():
    cust_name = pyip.inputRegex(
        r"^([A-Za-z]+\s[a-z]*\s*[A-Za-z]+)$", prompt="Enter customer full name: "
    ).split()

    if len(cust_name) == 3:
        first = cust_name[0].title()
        middle = cust_name[1].lower()
        last = cust_name[2].title()
        q1 = f"SELECT * FROM customer_tb WHERE FIRST_NAME='{first}' AND MIDDLE_NAME='{middle}' AND LAST_NAME='{last}'"
    elif len(cust_name) == 2:
        first = cust_name[0].title()
        last = cust_name[1].title()
        q1 = f"SELECT * FROM customer_tb WHERE FIRST_NAME='{first}' AND LAST_NAME='{last}'"

    columns = [
        "SSN_masked",
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
    ]
    return q1, columns


def set_cust_name():
    cust_name = (
        pyip.inputRegex(
            r"^([A-Za-z]+\s[a-z]*\s*[A-Za-z]+)$", prompt="Enter customer full name: "
        )
    ).split()

    if len(cust_name) == 3:
        first = cust_name[0].title()
        middle = cust_name[1].lower()
        last = cust_name[2].title()
        changes = (
            f"UPDATE CDW_SAPP_CUSTOMER SET FIRST_NAME = '{first}', "
            f"MIDDLE_NAME = '{middle}', "
            f"LAST_NAME = '{last}' "
        )
    elif len(cust_name) == 2:
        first = cust_name[0].title()
        last = cust_name[1].title()
        changes = (
            f"UPDATE CDW_SAPP_CUSTOMER SET FIRST_NAME = '{first}', "
            f"LAST_NAME = '{last}' "
        )
    return changes


def set_cust_card():
    new_card = pyip.inputRegex(
        r"^\d{16}$", prompt="Enter 16-digit new credit card number:"
    )
    changes = f"UPDATE CDW_SAPP_CUSTOMER SET CREDIT_CARD_NO = '{new_card}'"
    return changes


def set_cust_address():
    street = pyip.inputRegex(r"^[A-Za-z]+$", prompt="Enter street name: ").title()
    number = pyip.inputInt("Enter street number: ")
    city = pyip.inputRegex(r"^[A-Za-z]+$", prompt="Enter city name: ").title()
    state = pyip.inputRegex(
        r"^[A-Za-z]{2}$", prompt="Enter state abbrviation: "
    ).upper()
    country = pyip.inputRegex(r"^[A-Za-z]{3}", prompt="Enter country name: ").title()
    zipcode = pyip.inputInt("Enter zipcode: ")

    changes = (
        f"UPDATE CDW_SAPP_CUSTOMER SET FULL_STREET_ADDRESS = '{street}, {number}', "
        f"CUST_CITY = '{city}', "
        f"CUST_STATE = '{state}', "
        f"CUST_COUNTRY = '{country}', "
        f"CUST_ZIP = '{zipcode}' "
    )
    return changes


def set_cust_phone():
    new_phone = pyip.inputInt(
        prompt="Enter new phone as a 10 digit number (no dashes,spaces or parantheses): ",
        min=1000000000,
        max=9999999999,
    )
    new_phone = str(new_phone)
    new_phone = re.sub(r"(\d{3})(\d{3})(\d{4})", r"(\1)\2-\3", new_phone)
    changes = f"UPDATE CDW_SAPP_CUSTOMER SET CUST_PHONE = '{new_phone}' "
    return changes


def set_cust_email():
    new_email = pyip.inputRegex(
        r"^[\w.-]+@[\w]+.\w{3}$", prompt="Enter new email address: "
    )
    changes = f"UPDATE CDW_SAPP_CUSTOMER SET CUST_EMAIL = '{new_email}' "
    return changes


def modify_cust_record():
    ssn = pyip.inputRegex(
        r"\d{9}", prompt="Please enter the SSN for the record you wish to update: "
    )
    print(f"You are going to update information for SSN: *****{ssn[-4:]}")
    updates = ["Name", "Credit Card No", "Address", "Phone Number", "Email"]
    choice = pyip.inputMenu(updates, numbered=True)

    if choice == updates[0]:
        changes = set_cust_name()

    elif choice == updates[1]:
        changes = set_cust_card()

    elif choice == updates[2]:
        changes = set_cust_address()

    elif choice == updates[3]:
        changes = set_cust_phone()

    elif choice == updates[4]:
        changes = set_cust_email()

    changes = changes + f"WHERE SSN = '{ssn}';"

    return ssn, changes


def mysql_updater(sql_update):
    try:
        mydb = db.connect(
            host="localhost",
            user="root",
            password=mysql_pwd,
            database="creditcard_capstone",
        )
        cursor = mydb.cursor()
        cursor.execute(sql_update)
        mydb.commit()
        print(f"===> {cursor.rowcount} record updated")
    except Exception as e:
        print("Mysql update error! ", e)


def generate_bill():
    month = pyip.inputInt("Enter month as a number: ", min=1, max=12)
    year = pyip.inputInt("Enter year: ", min=1900, max=2023)
    credit_card_no = pyip.inputRegex(
        r"^\d{16}$", prompt="Enter 16-digit credit card number:"
    )
    month = str(month)
    year = str(year)
    date_string = year + month.zfill(2)

    q = (
        "SELECT CUST_CC_NO, TIMEID, SUM(TRANSACTION_VALUE) as Daily_Total"
        "FROM credit_tb "
        f"WHERE CUST_CC_NO = '{credit_card_no}' "
        f"AND TIMEID LIKE '{date_string}%' "
        "GROUP BY CUST_CC_NO, TIMEID;"
    )
    return q


def get_trans_interval():
    ssn = pyip.inputRegex(r"\d{9}", prompt="Please enter the SSN: ")
    date1 = pyip.inputInt("Enter beginning date YYYYMMDD: ", min=20000101, max=20230101)
    date2 = pyip.inputInt("Enter beginning date YYYYMMDD: ", min=20000101, max=20230101)
    q = (
        "SELECT CUST_CC_NO,  TIMEID, TRANSACTION_TYPE, TRANSACTION_VALUE, TRANSACTION_ID "
        "FROM credit_tb "
        f"WHERE CUST_SSN='{ssn}' "
        f"AND TIMEID between '{date1}' and '{date2}' "
        "ORDER BY TIMEID desc;"
    )
    return q
