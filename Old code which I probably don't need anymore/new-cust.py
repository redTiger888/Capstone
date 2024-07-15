# Importing the Libraries
import warnings
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, concat, substring
from datetime import datetime
import config  # Contains user and password for MySQL

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

def generate_monthly_bill(first_name, last_name, zip_code, last_4_cc, month, year, spark):
    # JDBC parameters
    jdbc_url = "jdbc:mysql://localhost:3306/creditcard_capstone"

    dbtable_query = f"(SELECT * FROM cdw_sapp_credit_card cc JOIN cdw_sapp_customer cust ON cust.CREDIT_CARD_NO = cc.CUST_CC_NO WHERE cust.FIRST_NAME = '{first_name}' AND cust.LAST_NAME = '{last_name}' AND cust.CUST_ZIP = '{zip_code}' AND SUBSTR(cc.CUST_CC_NO, -4) = '{last_4_cc}' AND cc.`month` = {month} AND cc.`year` = {year}) AS monthly_bill"

    user = config.user
    password = config.password

    try:
        df = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", dbtable_query) \
            .option("user", user) \
            .option("password", password) \
            .load()

        # Logging successful generation of monthly bill with timestamp
        logging.info("Monthly bill generated successfully at %s.", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        return df

    except Exception as e:
        print(f"Error generating monthly bill: {str(e)}")
        # Logging error while generating monthly bill with timestamp
        logging.error(f"Error generating monthly bill: {str(e)} at %s.", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        return None

def modify_account_details(df, spark):
    # JDBC parameters
    jdbc_url = "jdbc:mysql://localhost:3306/creditcard_capstone"

    table_name = "cdw_sapp_customer"

    user = config.user  # Replace with actual user from config or environment
    password = config.password  # Replace with actual password from config or environment

    try:
        # Creating a temporary view of the DataFrame to execute SQL queries
        df.createOrReplaceTempView("new_data_view")

        # Execute update query to modify account details
        update_query = f"""
        UPDATE {table_name} 
        SET FIRST_NAME = (SELECT FIRST_NAME FROM new_data_view), 
            LAST_NAME = (SELECT LAST_NAME FROM new_data_view) 
        WHERE FIRST_NAME = df.FIRST_NAME 
            AND LAST_NAME = df.LAST_NAME 
        """
        spark.sql(update_query)

        # Logging successful modification of account details with timestamp
        logging.info("Account details modified successfully at %s.", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        return True

    except Exception as e:
        print(f"Error modifying account details: {str(e)}")
        # Logging error while modifying account details with timestamp
        logging.error(f"Error modifying account details: {str(e)} at %s.", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        return False

def customer_operations(df, spark):
    while True:
        print("\nCustomer Operations Menu:")
        print("1. Check Account Details")
        print("2. Modify Account Details")
        print("3. Generate Monthly Bill")
        print("4. Display Transactions Between Two Dates")
        print("5. Exit")

        choice = input("Enter your choice (1-5): ").strip()

        if choice == '1':
            #1. Check Account Details"
            if df is not None:
                df.show(truncate=False)
        elif choice == '2':
            print("\nCustomer Info Modifcation Menu:")
            print("1. Modify Name")
            print("2. Modify Email")
            print("3. Modify Phone Number")
            print("4. Modify Address")
            print("5. Completed all modifications - Update Database")
            print("6. Exit")

            modify_choice = input("Enter your choice (1-6): ").strip()

            if modify_choice == '1':
                # Modify Name
                # print("Current First Name:", df.first_name)  # Print Current Data
                df.show()
                new_first_name = input("Enter New First Name: ").strip()
                new_middle_name = input("Enter New Middle Name: ").strip()
                new_last_name = input("Enter New Last Name: ").strip()

                # Update DataFrame df with the new names
                df = df.withColumn('FIRST_NAME', lit(new_first_name))
                df = df.withColumn('MIDDLE_NAME', lit(new_middle_name))
                df = df.withColumn('LAST_NAME', lit(new_last_name))

                df.show()

            elif modify_choice == '2':
                df.show()
                new_email = input("Enter New Email: ").strip()

                # Update DataFrame df with the new email
                df = df.withColumn('CUST_EMAIL', lit(new_email))
                df.show()
            elif modify_choice == '3':
                df.show()
                new_phone = input("Enter New Phone: ").strip()

                # Format the new phone number to (XXX)XXX-XXXX
                formatted_phone = concat(lit("("),
                         substring(lit(new_phone), 1, 3), lit(")"),
                         substring(lit(new_phone), 4, 3), lit("-"),
                         substring(lit(new_phone), 7, 4))

                # Update DataFrame df with the new phone
                df = df.withColumn('CUST_PHONE', formatted_phone)
                df.show()
            elif modify_choice == '4':
                df.show()
                new_door_num = input("Enter New Door Number: ").strip()
                new_street = input("Enter New Street Name: ").strip()
                new_city = input("Enter New City: ").strip()
                while True:
                    new_state = input("Enter New State: ").strip()
                    # Validate state abbreviation (2 letters)
                    if new_state.isalpha() and len(new_state) == 2:
                        break
                    else:
                        print("Invalid input. State should be 2 letter abbreviation.")

                new_country = input("Enter New Country: ").strip()
                
                while True:
                    new_zip = input("Enter New Zip: ").strip()
                    # Validate zip code (5 digits)
                    if len(new_zip) == 5 and new_zip.isdigit():
                        break
                    else:
                        print("Invalid input. Zip code should be 5 digits.")

                # Update DataFrame df with the new address
                df = df.withColumn('CUST_STREET', lit(new_door_num + ' ' + new_street))
                df = df.withColumn('CUST_CITY', lit(new_city))
                df = df.withColumn('CUST_STATE', lit(new_state))
                df = df.withColumn('CUST_COUNTRY', lit(new_country))
                df = df.withColumn('CUST_ZIP', lit(new_zip))

                df.show()
            elif modify_choice == '5':
                # Update Database
                if modify_account_details(df, spark):
                    print("Account details updated successfully!")
                else:
                    print("Failed to update account details. Please try again.")
            elif modify_choice == '6':
                continue
        elif choice == '3':
            #3. Generate Monthly Bill
            month = input("Enter the month (1-12): ").strip()
            year = input("Enter the year (YYYY): ").strip()

            # Validate month and year inputs
            if month.isdigit() and year.isdigit() and 1 <= int(month) <= 12 and len(year) == 4:
                if df is not None:
                    monthly_bill_df = generate_monthly_bill(first_name, last_name, zip_code, last_4_cc, int(month), int(year), spark)
                    if monthly_bill_df is not None:
                        monthly_bill_df.show(truncate=False)
                else:
                    print("No customer data found. Please check your inputs and try again.")
            else:
                print("Invalid input. Please enter a valid month (1-12) and year (YYYY).")
        elif choice == '4':
            #4. Display Transactions Between Two Dates
            start_date = input("Enter the start date (YYYY-MM-DD): ").strip()
            end_date = input("Enter the end date (YYYY-MM-DD): ").strip()

            # Validate start_date and end_date inputs
            try:
                datetime.strptime(start_date, '%Y-%m-%d')
                datetime.strptime(end_date, '%Y-%m-%d')
            except ValueError:
                print("Incorrect date format, should be YYYY-MM-DD")
                continue

            # Add logic to display transactions between two dates
            print(f"Displaying transactions between {start_date} and {end_date}")

        elif choice == '5':
            print("Exiting Customer Operations. Goodbye!")
            break

        else:
            print("Invalid input. Please enter a number between 1 and 5.")

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("CustomerModule") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    # Prompt for customer details
    first_name, last_name, zip_code, last_4_cc = prompt_for_customer_details()

    # Check if customer is valid
    is_valid, df = is_customer_valid(first_name, last_name, zip_code, last_4_cc, spark)

    if is_valid:
        print("Customer found. Performing operations...")
        customer_operations(df, spark)
    else:
        print("Customer not found. Exiting...")

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
