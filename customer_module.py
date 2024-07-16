# Importing the Libraries
import warnings
import logging
import mysql.connector
from mysql.connector import Error
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, concat, current_timestamp, initcap, lower, col, when, length, lpad
from datetime import datetime
import config  # Contains user and password for MySQL

from pyspark.sql.types import StringType
warnings.filterwarnings("ignore")  # Suppress specific warnings
logging.basicConfig(filename='customer_module.log', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

def prompt_for_customer_details():
    while True:
        first_name = input("Enter First Name: ").strip()
        if first_name.isalpha():  # Check if all characters are alphabetic
            break
        else:
            print("Invalid input. First name should contain only alphabetic characters.")
    
    while True:
        last_name = input("Enter Last Name: ").strip()
        if last_name.isalpha():  # Check if all characters are alphabetic
            break
        else:
            print("Invalid input. Last name should contain only alphabetic characters.")
    
    while True:
        zip_code = input("Enter ZIP Code (for example: 01824): ").strip()
        if zip_code.isdigit() and len(zip_code) == 5:  # Assuming ZIP code is 5 digits
            break
        else:
            print("Invalid ZIP Code. Please enter a 5-digit numeric ZIP Code.")
    
    while True:
        last_4_cc = input("Enter Last 4 Digits of Credit Card Number: ").strip()
        if last_4_cc.isdigit() and len(last_4_cc) == 4:  # Assuming last 4 digits of CC are exactly 4 digits
            break
        else:
            print("Invalid input. Please enter exactly 4 digits for the last 4 digits of the credit card number.")
    
    return first_name, last_name, zip_code, last_4_cc

def is_customer_valid(first_name, last_name, zip_code, last_4_cc, spark):
    jdbc_url = "jdbc:mysql://localhost:3306/creditcard_capstone"
    query = f"SELECT distinct cust.* FROM cdw_sapp_customer cust " \
            f"JOIN cdw_sapp_credit_card cc ON cust.CREDIT_CARD_NO = cc.CUST_CC_NO " \
            f"WHERE cust.FIRST_NAME = '{first_name}' AND cust.LAST_NAME = '{last_name}' " \
            f"AND cust.CUST_ZIP = '{zip_code}' AND SUBSTR(cc.CUST_CC_NO, -4) = '{last_4_cc}'"

    user = config.user
    password = config.password

    try:
        df = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("query", query) \
            .option("user", user) \
            .option("password", password) \
            .load()

        if not df.isEmpty():
            print()
            print("Customer Found!")
            print(f"Name: {df.select('FIRST_NAME').first()[0]} {df.select('LAST_NAME').first()[0]}")
            print(f"Address: {df.select('FULL_STREET_ADDRESS').first()[0]}, {df.select('CUST_CITY').first()[0]}, {df.select('CUST_STATE').first()[0]} {df.select('CUST_ZIP').first()[0]}")
            print(f"Phone: {df.select('CUST_PHONE').first()[0]}")
            print(f"Email: {df.select('CUST_EMAIL').first()[0]}")
            print()
            return True, df  # Return True and the DataFrame with matching records
        else:
            return False, None  # Return False and None if no records found

    except Exception as e:
        error_msg = f"Error: Customer not found in database: {str(e)}"
        print(error_msg)
        logging.error("%s at %s.", error_msg, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        return False, None  # Return False and None in case of any exception

# Function to modify account details in a MySQL table based on data from a DataFrame
def update_customer_in_db(old_df, modified_df):
    try:
  
        host = 'localhost'
        user = config.user
        password = config.password
        database_name = "creditcard_capstone"
        table_name = "cdw_sapp_customer"

        # Establish a connection to the MySQL server
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database_name
        )
    
        # Check if the connection is established
        if connection.is_connected():
            cursor = connection.cursor()  # Create a cursor object to interact with the database
            
            # Log MYSQL connection established
            logging.info(f"MySQL connection is established for MOdifying Customer Details at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            # # Ensure the DataFrame has exactly one row (record)
            # if len(old_df) != 1:
            #     raise ValueError("DataFrame must contain exactly one record for modifying account details.")
            
            # Fetch the new and old first names from the dataframes
            # old_df.show(5)
            old_fn = old_df.select("FIRST_NAME").first()[0]
            old_ln = old_df.select("LAST_NAME").first()[0]
            old_zp = old_df.select("CUST_ZIP").first()[0]
            old_cc = old_df.select("CREDIT_CARD_NO").first()[0]

            # modified_df.show(5)
            # Convert FIRST_NAME to Title Case
            modified_df = modified_df.withColumn("FIRST_NAME", initcap(modified_df["FIRST_NAME"]))
            new_fn = modified_df.select("FIRST_NAME").first()[0]
            # Convert MIDDLE_NAME to Lower Case
            modified_df = modified_df.withColumn("MIDDLE_NAME", lower(modified_df["MIDDLE_NAME"]))
            new_mn = modified_df.select("MIDDLE_NAME").first()[0]
            # Convert LAST_NAME to Title Case
            modified_df = modified_df.withColumn("LAST_NAME", initcap(modified_df["LAST_NAME"]))
            new_ln = modified_df.select("LAST_NAME").first()[0]
            new_apt_n = modified_df.select("APT_NO").first()[0]
            new_st_n = modified_df.select("STREET_NAME").first()[0]

            # Create FULL_STREET_ADDRESS by concatenating APT_NO and STREET_NAME
            modified_df = modified_df.withColumn("FULL_STREET_ADDRESS", concat(col("APT_NO"), lit(", "), col("STREET_NAME")).cast(StringType()))
            new_full_st = modified_df.select("FULL_STREET_ADDRESS").first()[0]
            new_city = modified_df.select("CUST_CITY").first()[0]
            new_state = modified_df.select("CUST_STATE").first()[0]
            new_country = modified_df.select("CUST_COUNTRY").first()[0]
            new_zp = modified_df.select("CUST_ZIP").first()[0]
            new_phone = modified_df.select("CUST_PHONE").first()[0]
            new_email = modified_df.select("CUST_EMAIL").first()[0]

            modified_df = modified_df.withColumn("LAST_UPDATED", current_timestamp())
            now = modified_df.select("LAST_UPDATED").first()[0]

            # print(old_fn)
            # print(new_fn)
            # SQL query to execute
            update_query = f"UPDATE {table_name} " \
                        f"SET FIRST_NAME = '{new_fn}', MIDDLE_NAME = '{new_mn}', LAST_NAME = '{new_ln}', " \
                        f"APT_NO = '{new_apt_n}', STREET_NAME = '{new_st_n}', FULL_STREET_ADDRESS = '{new_full_st}', " \
                        f"CUST_CITY = '{new_city}', CUST_STATE = '{new_state}', CUST_COUNTRY = '{new_country }', " \
                        f"CUST_ZIP = '{new_zp}', CUST_PHONE = '{new_phone}', CUST_EMAIL = '{new_email}', " \
                        f"LAST_UPDATED = '{now}' " \
                        f"WHERE FIRST_NAME = '{old_fn}' AND " \
                            f"LAST_NAME = '{old_ln}' AND CUST_ZIP = '{old_zp}' AND "\
                            f"CREDIT_CARD_NO = '{old_cc}' "
            
            # print(update_query)

            # Execute the update query with parameters
            cursor.execute(update_query)

            # Commit the changes to the database
            connection.commit()

            # Log successful update
            logging.info(f"Customer details for {new_fn} modified successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Customer details for {new_fn} {new_ln} zip: {new_zp} modified successfully!")
            print()

    except mysql.connector.Error as err:
        # Handle MySQL errors for modify_account_details
        logging.error(f"Error connecting to MySQL in modify_account_details: {err}")
    except ValueError as ve:
        # Handle ValueError for incorrect DataFrame length
        logging.error(f"ValueError: {ve}")
    except Exception as e:
        # Handle other exceptions for modify_account_details
        logging.error(f"Error in modify_account_details: {e}")
    
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()  # Close the cursor
            connection.close()  # Close the connection to the MySQL server
            # Log closing MYSQL connection
            logging.info(f"MySQL connection is closed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Function to generate monthly bill for a customer for a given month and year
def generate_monthly_bill(df, month, year, spark):
    # JDBC parameters

    jdbc_url = "jdbc:mysql://localhost:3306/creditcard_capstone"

    # Extracting necessary details from the DataFrame
    if df is not None and not df.rdd.isEmpty():

        first_name = df.select("FIRST_NAME").first()[0]
        last_name = df.select("LAST_NAME").first()[0]
        zip_code = df.select("CUST_ZIP").first()[0]
        cc_no = df.select("CREDIT_CARD_NO").first()[0]
    else:
        print("Customer DataFrame is empty or None.")
        return None

    # if df is not None:
    #     df.show(truncate=False)

    dbtable_query = f"(SELECT TRANSACTION_TYPE, TRANSACTION_VALUE, BRANCH_CODE, TIMEID FROM cdw_sapp_credit_card cc JOIN cdw_sapp_customer cust ON cust.CREDIT_CARD_NO = cc.CUST_CC_NO WHERE cust.FIRST_NAME = '{first_name}' AND cust.LAST_NAME = '{last_name}' AND cust.CUST_ZIP = '{zip_code}' AND cc.CUST_CC_NO = '{cc_no}' AND cc.month = {month} AND cc.year = {year}) AS monthly_bill"

    user = config.user
    password = config.password

    try:
        bill_df = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", dbtable_query) \
            .option("user", user) \
            .option("password", password) \
            .load()
        print()
        print(f"Printing Transaction details for {first_name} {last_name} for month: {month} and year: {year}")
        bill_df.show()
        # Logging successful generation of monthly bill with timestamp
        logging.info("Monthly bill generated successfully at %s.", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        return bill_df

    except Exception as e:
        print(f"Error generating monthly bill: {str(e)}")
        # Logging error while generating monthly bill with timestamp
        logging.error(f"Error generating monthly bill for {first_name} {last_name} for {month}/{year}: {str(e)} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        return None

def capture_customer_modifications(df):
    old_df = df.select("FIRST_NAME", "LAST_NAME", "CUST_ZIP", "CREDIT_CARD_NO")
    # old_df.show()
    print("Current Data for Customer: ")
    print(f"Name: {df.select('FIRST_NAME').first()[0]} {df.select('MIDDLE_NAME').first()[0]} {df.select('LAST_NAME').first()[0]}")
    print(f"Address:  {df.select('FULL_STREET_ADDRESS').first()[0]}, {df.select('CUST_CITY').first()[0]}, {df.select('CUST_STATE').first()[0]} {df.select('CUST_ZIP').first()[0]}")
    print(f"Country: {df.select('CUST_COUNTRY').first()[0]}")
    print(f"Phone: {df.select('CUST_PHONE').first()[0]}")
    print(f"Email: {df.select('CUST_EMAIL').first()[0]}")
    print(f"Last Updated: {df.select('LAST_UPDATED').first()[0]}")

    while True:
        print("\n--- Modify Customer Details ---")
        print("Select the field you want to modify:")
        print("1. First Name")
        print("2. Middle Name")
        print("3. Last Name")
        print("4. Door Number")
        print("5. Street Name")
        print("6. City")
        print("7. State")
        print("8. Country")
        print("9. ZIP Code")
        print("10. Phone Number")
        print("11. Email Address")
        print("12. Exit")

        choice = input("Enter your choice (1-12): ").strip()

        if choice == '1':
            new_value = input("Enter new First Name: ").strip()
            df = df.withColumn("FIRST_NAME", initcap(lit(new_value)))
        elif choice == '2':
            new_value = input("Enter new Middle Name: ").strip()
            df = df.withColumn("MIDDLE_NAME", lower(lit(new_value)))
        elif choice == '3':
            new_value = input("Enter new Last Name: ").strip()
            df = df.withColumn("LAST_NAME", initcap(lit(new_value)))
        elif choice == '4':
            new_value = input("Enter new Door Number: ").strip()
            df = df.withColumn("APT_NO", lit(new_value))
            df = df.withColumn("FULL_STREET_ADDRESS", concat(col("APT_NO"), lit(", "), col("STREET_NAME")).cast(StringType()))
        elif choice == '5':
            new_value = input("Enter new Street Name: ").strip()
            df = df.withColumn("STREET_NAME", lit(new_value))
            df = df.withColumn("FULL_STREET_ADDRESS", concat(col("APT_NO"), lit(", "), col("STREET_NAME")).cast(StringType()))
        elif choice == '6':
            new_value = input("Enter new City: ").strip()
            df = df.withColumn("CUST_CITY", lit(new_value))
        elif choice == '7':
            new_value = input("Enter new State: ").strip()
            if len(new_value) == 2 and new_value.isalpha():
                df = df.withColumn("CUST_STATE", lit(new_value))
            else:
                print("Invalid State code. Please enter a 2-letter code (e.g., CA for California).")
        elif choice == '8':
            new_value = input("Enter new Country: ").strip()
            df = df.withColumn("CUST_COUNTRY", lit(new_value))
        elif choice == '9':
            new_value = input("Enter new ZIP Code: ").strip()
            # Clean and format CUST_ZIP
            new_value = when(length(lit(new_value)) == 4, lpad(lit(new_value), 5, "0")).otherwise(lit(new_value))
            df = df.withColumn("CUST_ZIP", new_value)
        elif choice == '10':
            new_value = input("Enter new Phone Number: ").strip()
            # Clean and format CUST_PHONE
            new_value = "(" + new_value[:3] + ")" + new_value[3:6] + "-" + new_value[6:]
            df = df.withColumn("CUST_PHONE", lit(new_value))
        elif choice == '11':
            new_value = input("Enter new Email Address: ").strip()
            df = df.withColumn("CUST_EMAIL", lit(new_value))
        elif choice == '12':
            print("Exiting Modify Customer Details.")
            break
        else:
            print("Invalid choice. Please enter a number between 1 and 12.")

    modified_df = df.select("FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "APT_NO", "STREET_NAME", "CUST_CITY", "CUST_STATE",
                            "CUST_COUNTRY", "CUST_ZIP", "CUST_PHONE", "CUST_EMAIL", "FULL_STREET_ADDRESS", "LAST_UPDATED")

    print()
    print("Updated Data for Customer: ")
    print(f"Name: {modified_df.select('FIRST_NAME').first()[0]} {modified_df.select('MIDDLE_NAME').first()[0]} {modified_df.select('LAST_NAME').first()[0]}")
    print(f"Address:  {modified_df.select('FULL_STREET_ADDRESS').first()[0]}, {modified_df.select('CUST_CITY').first()[0]}, {modified_df.select('CUST_STATE').first()[0]} {modified_df.select('CUST_ZIP').first()[0]}")
    print(f"Country: {modified_df.select('CUST_COUNTRY').first()[0]}")
    print(f"Phone: {modified_df.select('CUST_PHONE').first()[0]}")
    print(f"Email: {modified_df.select('CUST_EMAIL').first()[0]}")
    print(f"Last Updated: {modified_df.select('LAST_UPDATED').first()[0]}")

    confirm = input("Do you want to confirm these changes? (yes/no): ").strip().lower()
    if confirm == 'yes':
        print("Changes confirmed.")
        return old_df, modified_df
    else:
        print("Modifications discarded. Please modify again.\n")
        capture_customer_modifications(df)

def login(spark):

    print("Welcome to Customer Service Portal")

    while True:
        print("\n--- Customer Module ---")
        print("1. Login to Customer Module")
        print("2. Exit")
        choice = input("Enter your choice (1-2): ").strip()
        
        if choice == '1':
            print("\nCustomer Verification:")
            first_name, last_name, zip_code, last_4_cc = prompt_for_customer_details()
            is_valid, df = is_customer_valid(first_name, last_name, zip_code, last_4_cc, spark)
            if is_valid:
                print("How can we help this customer? ")
                return df  # Return the DataFrame with customer details
            else:
                print("Invalid customer details. Please try again.")
        elif choice == '2':
            print("Exiting the Customer Module.")
            break
        else:
            print("Invalid choice. Please enter 1 or 2.")

    # Stop Spark session when done
    spark.stop()

def customer_module(df, spark):
    while True:
        print("\n--- Customer Details Module ---")
        print("1. Check Existing Customer Details")
        print("2. Modify Customer Details")
        print("3. Generate Monthly Bill")
        print("4. View All Transactions Between Two Dates")
        print("5. Exit")
        choice = input("Enter your choice (1-5): ").strip()

        if choice == '1':
            print("\n--- Existing Customer Details ---")
            print(f"Name: {df.select('FIRST_NAME').first()[0]} {df.select('MIDDLE_NAME').first()[0]} {df.select('LAST_NAME').first()[0]}")
            print(f"SSN: {df.select('SSN').first()[0]}")
            print(f"Credit Card No: {df.select('CREDIT_CARD_NO').first()[0]}")
            print(f"Address:  {df.select('FULL_STREET_ADDRESS').first()[0]}, {df.select('CUST_CITY').first()[0]}, {df.select('CUST_STATE').first()[0]} {df.select('CUST_ZIP').first()[0]}")
            print(f"Country: {df.select('CUST_COUNTRY').first()[0]}")
            print(f"Phone: {df.select('CUST_PHONE').first()[0]}")
            print(f"Email: {df.select('CUST_EMAIL').first()[0]}")
            print(f"Last Updated: {df.select('LAST_UPDATED').first()[0]}")

        elif choice == '2':
            print("\n--- Modify Customer Details ---")
            old_df, new_df = capture_customer_modifications(df)
            update_customer_in_db(old_df, new_df)
            print("Exiting the Customer Details Module, please re-login with updated credentials")
            choice == '5'
        elif choice == '3':
            print("\n--- Generate Monthly Bill ---")
            month = int(input("Enter Month (1-12): ").strip())
            year = int(input("Enter Year: ").strip())
            generate_monthly_bill(df, month, year, spark)
        elif choice == '4':
            print("\n--- View All Transactions Between Two Dates ---")
            start_date = input("Enter Start Date (YYYYMMDD): ").strip()
            end_date = input("Enter End Date (YYYYMMDD): ").strip()
            view_transactions_between_dates(df, start_date, end_date, spark)
        elif choice == '5':
            print("Exiting the Customer Details Module.")
            break
        else:
            print("Invalid choice. Please enter a number between 1 and 5.")

def view_transactions_between_dates(df, start_date, end_date, spark):
    jdbc_url = "jdbc:mysql://localhost:3306/creditcard_capstone"
    first_name = df.select("FIRST_NAME").first()[0]
    last_name = df.select("LAST_NAME").first()[0]
    zip_code = df.select("CUST_ZIP").first()[0]
    cc_no = df.select("CREDIT_CARD_NO").first()[0]
    
    query = f"(SELECT TRANSACTION_TYPE, TRANSACTION_VALUE, BRANCH_CODE, TIMEID FROM cdw_sapp_credit_card cc " \
            f"JOIN cdw_sapp_customer cust ON cust.CREDIT_CARD_NO = cc.CUST_CC_NO " \
            f"WHERE cust.FIRST_NAME = '{first_name}' AND cust.LAST_NAME = '{last_name}' " \
            f"AND cust.CUST_ZIP = '{zip_code}' AND cust.CREDIT_CARD_NO = '{cc_no}' " \
            f"AND cc.TIMEID >= '{start_date}' " \
            f"AND cc.TIMEID <= '{end_date}' " \
            f"order by CC.TIMEID DESC) AS transactions"
    
    # print(query)

    user = config.user
    password = config.password

    try:
        transactions_df = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", query) \
            .option("user", user) \
            .option("password", password) \
            .load()

        if transactions_df.count() == 0:
            print(f"No transactions found for {first_name} {last_name} from {start_date} to {end_date}.")
            # Log the absence of transactions
            logging.warning(f"No transactions found for {first_name} {last_name} from {start_date} to {end_date} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        else:
            print()
            print(f"Printing Transaction details for {first_name} {last_name} from: {start_date} to: {end_date}")
           # Count the number of records in the DataFrame
            num_records = transactions_df.count()

            # Show all records without truncating
            transactions_df.show(num_records, truncate=False)

            # Logging successful retrieval of transactions
            logging.info(f"Transactions retrieved successfully for {first_name} {last_name} from {start_date} to {end_date} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    except Exception as e:
        print(f"Error retrieving transactions: {str(e)}")
        # Logging error while retrieving transactions
        logging.error(f"Error retrieving transactions for {first_name} {last_name} from {start_date} to {end_date}: {str(e)} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Function to create a Spark session
def create_spark_session(app_name='Customer Operations'):
    return SparkSession.builder.appName(app_name).getOrCreate()

def main():
    # Start Spark session
    spark = create_spark_session()

    # Welcome message
    print("Welcome to Customer Service Portal")

    # Prompt the user to log in and validate their details
    df = login(spark)
    if df is not None:
        customer_module(df, spark)
    
    # Stop Spark session when done
    spark.stop()

# Main execution
if __name__ == "__main__":
    main()

