# table names for spark -> mysql database load
table_names = {
    "credit_transformed": "CDW_SAPP_CREDIT_CARD",
    "branch_transformed": "CDW_SAPP_BRANCH",
    "customer_transformed": "CDW_SAPP_CUSTOMER",
    "loan_transformed": "CDW_SAPP_loan_application",
}

# Mapping dictionaries:
# tuple: (old_col, new_col, type)
# Mapping for file -> sparkDF transformations
column_map = {
    "credit": [
        ("CREDIT_CARD_NO", "CUST_CC_NO", "string"),
        # (['DAY','MONTH','YEAR'],'TIMEID','string'),
        ("CUST_SSN", "CUST_SSN", "integer"),
        ("BRANCH_CODE", "BRANCH_CODE", "integer"),
        ("TRANSACTION_TYPE", "TRANSACTION_TYPE", "string"),
        ("TRANSACTION_VALUE", "TRANSACTION_VALUE", "float"),
        ("TRANSACTION_ID", "TRANSACTION_ID", "integer"),
    ],
    "branch": [
        ("BRANCH_CODE", "BRANCH_CODE", "integer"),
        ("BRANCH_NAME", "BRANCH_NAME", "string"),
        ("BRANCH_STREET", "BRANCH_STREET", "string"),
        ("BRANCH_CITY", "BRANCH_CITY", "string"),
        ("BRANCH_STATE", "BRANCH_STATE", "string"),
        ("BRANCH_ZIP", "BRANCH_ZIP", "integer"),
        ("BRANCH_PHONE", "BRANCH_PHONE", "string"),
        ("LAST_UPDATED", "LAST_UPDATED", "timestamp"),
    ],
    "customer": [
        ("SSN", "SSN", "integer"),
        ("FIRST_NAME", "FIRST_NAME", "string"),
        ("MIDDLE_NAME", "MIDDLE_NAME", "string"),
        ("LAST_NAME", "LAST_NAME", "string"),
        ("CREDIT_CARD_NO", "CREDIT_CARD_NO", "string"),
        # (['STREET_NAME','APT_NO'],'FULL_STREET_ADDRESS','string'),
        ("CUST_CITY", "CUST_CITY", "string"),
        ("CUST_STATE", "CUST_STATE", "string"),
        ("CUST_COUNTRY", "CUST_COUNTRY", "string"),
        ("CUST_ZIP", "CUST_ZIP", "integer"),
        ("CUST_PHONE", "CUST_PHONE", "string"),
        ("CUST_EMAIL", "CUST_EMAIL", "string"),
        ("LAST_UPDATED", "LAST_UPDATED", "timestamp"),
    ],
}
# Mapping for SparkDF -> MySQL variable types
column_types_mysql = {
    "customer_transformed": "SSN INT, "
    "FIRST_NAME VARCHAR(50), "
    "MIDDLE_NAME VARCHAR(50), "
    "LAST_NAME VARCHAR(50), "
    "CREDIT_CARD_NO VARCHAR(16), "
    "FULL_STREET_ADDRESS VARCHAR(100), "
    "CUST_CITY VARCHAR(50), "
    "CUST_STATE VARCHAR(10), "
    "CUST_COUNTRY VARCHAR(50), "
    "CUST_ZIP INT, "
    "CUST_PHONE VARCHAR(50), "
    "CUST_EMAIL VARCHAR(100), "
    "LAST_UPDATED TIMESTAMP",
    "credit_transformed": "CUST_CC_NO VARCHAR(16), "
    "TIMEID VARCHAR(10), "
    "CUST_SSN INT, "
    "BRANCH_CODE INT, "
    "TRANSACTION_TYPE VARCHAR(50), "
    "TRANSACTION_VALUE DOUBLE, "
    "TRANSACTION_ID INT",
    "branch_transformed": "BRANCH_CODE INT, "
    "BRANCH_NAME VARCHAR(50), "
    "BRANCH_STREET VARCHAR(100), "
    "BRANCH_CITY VARCHAR(50), "
    "BRANCH_STATE VARCHAR(10), "
    "BRANCH_ZIP INT, "
    "BRANCH_PHONE VARCHAR(50), "
    "LAST_UPDATED TIMESTAMP",
    "loan_transformed": "Application_ID VARCHAR(20),"
    "Gender VARCHAR(20),"
    "Married VARCHAR(20),"
    "Dependents VARCHAR(20),"
    "Education VARCHAR(20),"
    "Self_Employed VARCHAR(20),"
    "Credit_History INT, "
    "Property_Area VARCHAR(20),"
    "Income VARCHAR(20),"
    "Application_Status VARCHAR(20)",
}
