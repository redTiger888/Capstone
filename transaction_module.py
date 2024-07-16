# Importing the Libraries
import warnings
import logging

from prettytable import PrettyTable

from pyspark.sql import SparkSession
from datetime import datetime
import config  # Contains user and password for MySQL

warnings.filterwarnings("ignore") # Suppress specific warnings
# logging.basicConfig(level=logging.WARN) # Set the logging level

# Configure logging
logging.basicConfig(filename='transaction_module.log', format='%(asctime)s - %(message)s', level=logging.INFO)

# Function to Prompt user to enter Zip code and verify format
def get_zipcode():
    while True:
        zip_code = input("Enter zip code (for example: 01824): ").strip()

        # Verify zip code has 5 digits
        if zip_code.isdigit() and len(zip_code) == 5:
            return zip_code
        else:
            print("Invalid zip code entered. Please enter a 5-digit numeric zip code.")

# Function to Prompt user to enter Month and Year and check validity
def get_month_year():
    while True:
        month = input("Enter Month (1-12): ").strip()
        
        # Verify month is between 1 & 12
        if month.isdigit() and 1 <= int(month) <= 12:
            break  # Valid month, exit loop
        else:
            print("Invalid input. Please enter a valid month (1-12)!")
    
    while True:
        year = input("Enter Year (for example: 2024): ").strip()
        
        # Verify year has 4 digits
        if year.isdigit() and len(year) == 4:
            break  # Valid year, exit the loop
        else:
            print("Invalid input. Please enter a valid 4-digit numeric year!")
        
    return int(month), int(year)

# Function to query the MySQL database based on user input
def query_transactions(zip_code, month, year, spark):
    # JDBC parameters
    jdbc_url = "jdbc:mysql://localhost:3306/creditcard_capstone"

    dbtable_query = f'''(SELECT timeid AS Date, TRANSACTION_TYPE AS Type, 
                                TRANSACTION_VALUE AS Amount, cust.FIRST_NAME AS "First Name", 
                                cust.LAST_NAME AS "Last Name"
                    FROM cdw_sapp_credit_card cc
                    JOIN cdw_sapp_customer cust ON cust.CREDIT_CARD_NO = cc.CUST_CC_NO
                    WHERE cc.month = {month} AND cc.year = {year} AND cust_zip = '{zip_code}'
                    ORDER BY day DESC) AS filtered_transactions'''
    
    user = config.user
    password = config.password

    # Create a DataFrame reader using the JDBC connection and query
    try:
        df = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", dbtable_query) \
            .option("user", user) \
            .option("password", password) \
            .load()

         # Logging successful query of transactions with timestamp
        logging.info("Query ran successfully: %s at %s.", dbtable_query, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        return df

    except Exception as e:
        print(f"Error querying data: {str(e)}")
        logging.error("Error querying data: %s at %s.", str(e), datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        return None

# Master function to orchestrate the transaction detail retrieval
    # Prompts the user for zip code, month, and year
    # Calls query_transactions() to fetch data based on user input
    # Displays the retrieved data if successful; otherwise, shows an error message.
def get_transaction_detail(spark):
    print("Welcome to the Transactions Details Module")
    print("This module lists all transactions of ALL customers for the entered zip code in the specified month and year")
    zip_code = get_zipcode() 
    month, year = get_month_year() 

    df = query_transactions(zip_code, month, year, spark)

    # if df is not None:
    #     df.show(df.count(), truncate=False)
    # else:
    #     print("No data retrieved or error occurred.")

    print()
    print(f"All transactions for zip: {zip_code} in Month: {month} and Year: {year}")
    if df is not None and df.count() > 0:
        pt = PrettyTable(['Date', 'Type', 'Amount', 'First Name', 'Last Name'])
        for row in df.collect():
            pt.add_row([row['Date'], row['Type'], row['Amount'], row['First Name'], row['Last Name']])
        print(pt)
    else:
        print("No data retrieved or error occurred.")


# Main
if __name__ == "__main__":
    # Creating Spark Session
    spark = SparkSession.builder \
        .appName('Query Transactions') \
        .getOrCreate()

    # Set logging level to ERROR
    spark.sparkContext.setLogLevel("ERROR")

    get_transaction_detail(spark)
    spark.stop()
