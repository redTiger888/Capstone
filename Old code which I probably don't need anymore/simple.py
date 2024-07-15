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


# def modify_account_details(df, spark):
#     # JDBC parameters
#     jdbc_url = "jdbc:mysql://localhost:3306/creditcard_capstone"

#     table_name = "cdw_sapp_customer"

#     user = config.user
#     password = config.password

#     try:
#         df.show(5)
#         # Update the 'CUST_CITY' column in the DataFrame
#         updated_df = df.withColumn('CUST_CITY', lit('Fun Land'))

#         updated_df.show(5)

#         # Write the updated DataFrame to MySQL table using JDBC
#         updated_df.write \
#             .format("jdbc") \
#             .option("url", jdbc_url) \
#             .option("dbtable", table_name) \
#             .option("user", user) \
#             .option("password", password) \
#             .mode("overwrite") \
#             .save()
        
#         # Logging successful modification of account details with timestamp
#         logging.info("Account details modified successfully at %s.", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

#         return True

#     except Exception as e:
#         print(f"Error modifying account details: {str(e)}")
#         # Logging error while modifying account details with timestamp
#         logging.error(f"Error modifying account details: {str(e)} at %s.", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

#         return False


# def modify_account_details(old_df, modified_df, spark):
#     # JDBC parameters
#     jdbc_url = "jdbc:mysql://localhost:3306/creditcard_capstone"
#     table_name = "cdw_sapp_customer"
#     user = config.user  # Assuming config.user is defined elsewhere
#     password = config.password  # Assuming config.password is defined elsewhere

#     try:
#         # Display the dataframes to verify content (for debugging purposes)
#         modified_df.select("FIRST_NAME").show()
#         old_df.select("FIRST_NAME").show()

#         # Fetch the new and old first names from the dataframes
#         new_fn = modified_df.select("FIRST_NAME").first()[0]
#         old_fn = old_df.select("FIRST_NAME").first()[0]

#         # SQL query to execute
#         update_query = f"UPDATE {table_name} " \
#                        f"SET FIRST_NAME = '{new_fn}' " \
#                        f"WHERE FIRST_NAME = '{old_fn}'"

#         # Execute the SQL query using Spark JDBC write
#         jdbc_properties = {
#             "url": jdbc_url,
#             "driver": "com.mysql.jdbc.Driver",
#             "user": user,
#             "password": password
#         }

#         spark.write.jdbc(
#             url=jdbc_url,
#             table=table_name,
#             mode="overwrite",  # Mode can be overwrite or append based on your requirement
#             properties=jdbc_properties
#         )

#         # Logging successful modification of account details with timestamp
#         logging.info("Account details modified successfully at %s.", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

#         return True

#     except Exception as e:
#         print(f"Error modifying account details: {str(e)}")
#         # Logging error while modifying account details with timestamp
#         logging.error(f"Error modifying account details: {str(e)} at %s.", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

#         return False


# def modify_account_details(old_df,modified_df, spark):
#     # JDBC parameters
#     jdbc_url = "jdbc:mysql://localhost:3306/creditcard_capstone"
#     table_name = "cdw_sapp_customer"
#     user = config.user
#     password = config.password

#     try:
#         modified_df.select("FIRST_NAME").show()
#         old_df.select("FIRST_NAME").show()
#         new_fn = modified_df.select("FIRST_NAME").first()[0]
#         old_fn = old_df.select("FIRST_NAME").first()[0]

#         # SQL query to execute
#         update_query = f"UPDATE {table_name} " \
#         f"SET FIRST_NAME = '{new_fn}' " \
#         f"WHERE FIRST_NAME = '{old_fn}'" 


#         # update_query = f"""
#         # UPDATE {table_name}
#         # SET CUST_CITY = `{modified_df.select("CUST_CITY").first()[0]}`, 
#         #     FIRST_NAME = `{modified_df.select("FIRST_NAME").first()[0]}`, 
#         #     LAST_NAME = '{modified_df.select("LAST_NAME").first()[0]}', 
#         #     CUST_STATE = '{modified_df.select("CUST_STATE").first()[0]}'
#         # WHERE FIRST_NAME = '{old_df.select("FIRST_NAME").first()[0]}'
#         # AND LAST_NAME = '{old_df.select("LAST_NAME").first()[0]}'
#         # AND CUST_ZIP = '{old_df.select("CUST_ZIP").first()[0]}'
#         # AND CUST_CC_NO = '{old_df.select("CREDIT_CARD_NO").first()[0]}'
#         # """

#         print(update_query)

#         # Execute the SQL query using Spark JDBC
#         spark.read \
#             .format("jdbc") \
#             .option("url", jdbc_url) \
#             .option("query", update_query) \
#             .option("user", user) \
#             .option("password", password) \
#             .load()

#         # Logging successful modification of account details with timestamp
#         logging.info("Account details modified successfully at %s.", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

#         return True

#     except Exception as e:
#         print(f"Error modifying account details: {str(e)}")
#         # Logging error while modifying account details with timestamp
#         logging.error(f"Error modifying account details: {str(e)} at %s.", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

#         return False

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
            customer_df.show()

            updated_df = customer_df

            # Step 3: Make changes
            updated_df = updated_df.withColumn("CUST_CITY", lit("Fun Land"))  
            updated_df = updated_df.withColumn("FIRST_NAME", lit("Alexa"))
            updated_df = updated_df.withColumn("LAST_NAME", lit("Empress"))
            updated_df = updated_df.withColumn("CUST_STATE", lit("XX"))

            updated_df.show()
       
            # Step 4: Modify account details in the database
            print("\n=== Modifying Account Details ===")
            modification_successful = modify_account_details(customer_df, updated_df, spark)

            if modification_successful:
                print("Account details successfully modified.")
            else:
                print("Failed to modify account details.")

        else:
            print("Customer not found in the database. Unable to proceed with modifications.")

    except Exception as e:
        print(f"An error occurred: {str(e)}")

    finally:
        # Stop SparkSession
        spark.stop()

if __name__ == "__main__":
    main()
