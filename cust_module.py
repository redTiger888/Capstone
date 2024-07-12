# Importing the Libraries
import warnings
import logging
import pyspark
from pyspark.sql import SparkSession
from datetime import datetime
import config  # Contains user and password for MySQL

warnings.filterwarnings("ignore")  # Suppress specific warnings
logging.basicConfig(level=logging.WARN)  # Set the logging level

# Configure logging
logging.basicConfig(filename='customer_module.log', format='%(asctime)s - %(message)s', level=logging.INFO)

# Function to Prompt user to enter Customer Details
def get_customer_details():
    first_name = input("Enter First Name: ").strip()
    last_name = input("Enter Last Name: ").strip()
    zip_code = input("Enter ZIP Code (for example: 01824): ").strip()
    last_4_ssn = input("Enter Last 4 Digits of SSN: ").strip()
    last_4_cc = input("Enter Last 4 Digits of Credit Card Number: ").strip()
    return first_name, last_name, zip_code, last_4_ssn, last_4_cc

# Function to check existing account details of a customer
def check_account_details(first_name, last_name, zip_code, last_4_ssn, last_4_cc, spark):
    # JDBC parameters
    jdbc_url = "jdbc:mysql://localhost:3306/creditcard_capstone"

    dbtable_query = f"(SELECT * FROM cdw_sapp_customer cust JOIN cdw_sapp_credit_card cc ON cust.CREDIT_CARD_NO = cc.CUST_CC_NO WHERE cust.FIRST_NAME = '{first_name}' AND cust.LAST_NAME = '{last_name}' AND cust.CUST_ZIP = '{zip_code}' AND SUBSTR(cust.CUST_SSN, 6, 4) = '{last_4_ssn}' AND SUBSTR(cc.CUST_CC_NO, -4) = '{last_4_cc}') AS account_details"

    user = config.user
    password = config.password

    try:
        df = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", dbtable_query) \
            .option("user", user) \
            .option("password", password) \
            .load()

        # Logging successful query of account details with timestamp
        logging.info("Account details fetched successfully at %s.", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        return df

    except Exception as e:
        print(f"Error querying account details: {str(e)}")
        # Logging error while querying account details with timestamp
        logging.error(f"Error querying account details: {str(e)} at %s.", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        return None

# Function to modify existing account details of a customer
def modify_account_details(first_name, last_name, zip_code, last_4_ssn, last_4_cc, new_data, spark):
    # JDBC parameters
    jdbc_url = "jdbc:mysql://localhost:3306/creditcard_capstone"

    table_name = "cdw_sapp_customer"

    user = config.user
    password = config.password

    try:
        # Creating a temporary view of the DataFrame to execute SQL queries
        new_data.createOrReplaceTempView("new_data_view")

        # Execute update query to modify account details
        spark.sql(f"UPDATE {table_name} SET FIRST_NAME = (SELECT FIRST_NAME FROM new_data_view), LAST_NAME = (SELECT LAST_NAME FROM new_data_view) WHERE FIRST_NAME = '{first_name}' AND LAST_NAME = '{last_name}' AND CUST_ZIP = '{zip_code}' AND SUBSTR(CUST_SSN, 6, 4) = '{last_4_ssn}' AND SUBSTR(CUST_CC_NO, -4) = '{last_4_cc}'")

        # Logging successful modification of account details with timestamp
        logging.info("Account details modified successfully at %s.", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        return True

    except Exception as e: