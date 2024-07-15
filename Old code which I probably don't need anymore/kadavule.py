# Importing the Libraries
import warnings
import logging
import mysql.connector
from mysql.connector import Error
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, concat, current_timestamp
from datetime import datetime
import config  # Contains user and password for MySQL
from pyspark.sql.functions import initcap, lower, concat, col
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
            return True, df  # Return True and the DataFrame with matching records
        else:
            return False, None  # Return False and None if no records found

    except Exception as e:
        error_msg = f"Error: Customer not found in database: {str(e)}"
        print(error_msg)
        logging.error("%s at %s.", error_msg, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        return False, None  # Return False and None in case of any exception

# Function to modify account details in a MySQL table based on data from a DataFrame
def modify_account_details(old_df, modified_df):
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
            old_df.show(5)
            old_fn = old_df.select("FIRST_NAME").first()[0]
            old_ln = old_df.select("LAST_NAME").first()[0]
            old_zp = old_df.select("CUST_ZIP").first()[0]
            old_cc = old_df.select("CREDIT_CARD_NO").first()[0]

            modified_df.show(5)
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

            print(old_fn)
            print(new_fn)
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
            
            print(update_query)

            # Execute the update query with parameters
            cursor.execute(update_query)

            # Commit the changes to the database
            connection.commit()

            # Log successful update
            logging.info(f"Customer details for {new_fn} modified successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

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


def main():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("CustomerAccountModification") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    try:
        # Step 1: Prompt for customer details
        print("=== Enter Customer Details ===")
        first_name, last_name, zip_code, last_4_cc = prompt_for_customer_details()

        # Step 2: Verify customer details
        print("\n=== Verifying Customer Details ===")
        valid_customer, customer_df = is_customer_valid(first_name, last_name, zip_code, last_4_cc, spark)

        if valid_customer:
            print("Customer found in the database. Details:")
            # customer_df.show()

            updated_df = customer_df

            # Step 3: Make changes
            updated_df = updated_df.withColumn("CUST_CITY", lit("Fun Land"))  
            updated_df = updated_df.withColumn("FIRST_NAME", lit("Alexa"))
            updated_df = updated_df.withColumn("LAST_NAME", lit("Empress"))
            updated_df = updated_df.withColumn("CUST_STATE", lit("XX"))

            # updated_df.show()
       
            # Step 4: Modify account details in the database
            print("\n=== Modifying Account Details ===")
            modify_account_details(customer_df, updated_df)
            # modification_successful = modify_account_details(customer_df, updated_df)

            # if modification_successful:
            #     print("Account details successfully modified.")
            # else:
            #     print("Failed to modify account details.")

        else:
            print("Customer not found in the database. Unable to proceed with modifications.")

    except Exception as e:
        print(f"An error occurred: {str(e)}")

    finally:
        # Stop SparkSession
        spark.stop()

if __name__ == "__main__":
    main()
