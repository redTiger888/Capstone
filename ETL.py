#import libraries
import warnings
import pyspark
import config
import mysql.connector
from mysql.connector import Error
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql.functions import initcap, lower, concat, col, regexp_replace, lit, substring, lpad, when, length

# Suppress all warnings
warnings.filterwarnings("ignore")

# Configure logging
logging.basicConfig(filename='etl.log', format='%(asctime)s - %(message)s', level=logging.INFO)

# Function for extracting Branch data
def extract_branch(spark, json_file):
    try: 
        schema = StructType([
            StructField("BRANCH_CODE", IntegerType(), True),
            StructField("BRANCH_NAME", StringType(), True),
            StructField("BRANCH_STREET", StringType(), True),
            StructField("BRANCH_CITY", StringType(), True),
            StructField("BRANCH_STATE", StringType(), True),
            StructField("BRANCH_ZIP", StringType(), True),
            StructField("BRANCH_PHONE", StringType(), True),
            StructField("LAST_UPDATED", TimestampType(), True)
        ])
        
        # Create Spark df and extract the Dataset from json file 
        branch_df = spark.read.option("multiline", "true").schema(schema).json(json_file)
        
         # Logging successful extraction with timestamp
        logging.info("Data extraction from CDW_SAPP_BRANCH successful at %s.", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        
        return branch_df
    
    except Exception as e:
        logging.error(f"Error occurred in extract_branch function: {str(e)}") # logging of error to etl.log
        print(f"Error occurred: {str(e)}")  # user-friendly print statement for error
        raise

# Function for transforming Branch data
def transform_branch(branch_df):
    try: 
        # Change the format of phone number to (XXX)XXX-XXXX
        branch_df = branch_df.withColumn("BRANCH_PHONE", 
                                        concat(lit("("), substring("BRANCH_PHONE", 1, 3), lit(")"),
                                            substring("BRANCH_PHONE", 4, 3), lit("-"),
                                            substring("BRANCH_PHONE", 7, 4)))

        # Add logic to handle Zip code
        branch_df = branch_df.withColumn("BRANCH_ZIP", 
                                        when(branch_df["BRANCH_ZIP"].isNull(), "99999")  # Replace null values with "99999"
                                        .otherwise(                                   # Otherwise, if only 4 digit, add leading zero
                                            when(length(col("BRANCH_ZIP")) == 4, lpad(col("BRANCH_ZIP"), 5, "0"))  
                                            .otherwise(col("BRANCH_ZIP"))))
        
          # Logging successful transformation with timestamp
        logging.info("Data transformation for CDW_SAPP_BRANCH successful at %s.", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        
        return branch_df
    except Exception as e:
        logging.error(f"Error occurred in transform_branch function: {str(e)}")  # logging of error to etl.log
        print(f"Error occurred: {str(e)}")  # user-friendly print statement for error
        raise

# Function for extracting Customer data
def extract_customer(spark, json_file):
    try:
        schema = StructType([
            StructField("FIRST_NAME", StringType(), True),
            StructField("MIDDLE_NAME", StringType(), True),
            StructField("LAST_NAME", StringType(), True),
            StructField("SSN", IntegerType(), True),
            StructField("CREDIT_CARD_NO", StringType(), True),
            StructField("APT_NO", StringType(), True),
            StructField("STREET_NAME", StringType(), True),
            StructField("CUST_CITY", StringType(), True),
            StructField("CUST_STATE", StringType(), True),
            StructField("CUST_COUNTRY", StringType(), True),
            StructField("CUST_ZIP", StringType(), True),
            StructField("CUST_PHONE", StringType(), True),
            StructField("CUST_EMAIL", StringType(), True),
            StructField("LAST_UPDATED", TimestampType(), True)
        ])
        
        # Read JSON file into DataFrame
        cust_df = spark.read.option("multiline", "true").schema(schema).json(json_file)

        # Log successful extraction
        logging.info(f"Data extraction from {json_file} successful at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return cust_df
        
    
    except Exception as e:
        logging.error(f"Error occurred in extract_customer function: {str(e)}")
        print(f"Error occurred: {str(e)}")
        raise

# Function for Transforming Customer data
def transform_customer(cust_df):
    try:
        # Convert FIRST_NAME to Title Case
        cust_df = cust_df.withColumn("FIRST_NAME", initcap(cust_df["FIRST_NAME"]))
        
        # Convert MIDDLE_NAME to Lower Case
        cust_df = cust_df.withColumn("MIDDLE_NAME", lower(cust_df["MIDDLE_NAME"]))
        
        # Convert LAST_NAME to Title Case
        cust_df = cust_df.withColumn("LAST_NAME", initcap(cust_df["LAST_NAME"]))
        
        # Create FULL_STREET_ADDRESS by concatenating APT_NO and STREET_NAME
        cust_df = cust_df.withColumn("FULL_STREET_ADDRESS", concat(col("APT_NO"), lit(", "), col("STREET_NAME")).cast(StringType()))
        
        # Clean and format CUST_PHONE
        cust_df = cust_df.withColumn("CUST_PHONE", lpad(regexp_replace(col("CUST_PHONE"), "[^0-9]", ""), 10, '000'))
        cust_df = cust_df.withColumn("CUST_PHONE", 
                                     concat(lit("("), substring("CUST_PHONE", 1, 3), lit(")"),
                                            substring("CUST_PHONE", 4, 3), lit("-"),
                                            substring("CUST_PHONE", 7, 4)))
        
         # Log successful transformation
        logging.info(f"Data transformation for Customer successful at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
       
        return cust_df
    
    except Exception as e:
        logging.error(f"Error occurred in transform_customer function: {str(e)}")
        print(f"Error occurred: {str(e)}")
        raise

# Function for extracting Customer data
def extract_credit_card(spark, json_file):
    try:
        schema = StructType([
            StructField("TRANSACTION_ID", IntegerType(), True),
            StructField("DAY", IntegerType(), True),
            StructField("MONTH", IntegerType(), True),
            StructField("YEAR", IntegerType(), True),
            StructField("TIMEID", StringType(), True),
            StructField("CREDIT_CARD_NO", StringType(), True),
            StructField("CUST_SSN", IntegerType(), True),
            StructField("BRANCH_CODE", IntegerType(), True),
            StructField("TRANSACTION_TYPE", StringType(), True),
            StructField("TRANSACTION_VALUE", DoubleType(), True)
        ])
        
        credit_df = spark.read.option("multiline", "true").schema(schema).json(json_file)
        
        # Log successful extraction
        logging.info(f"Data extraction from {json_file} successful at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
        return credit_df
    
    except Exception as e:
        logging.error(f"Error occurred in Extract Customer function: {str(e)}")
        print(f"Error occurred: {str(e)}")
        raise

# Function for Transforming Credit Card Transaction data
def transform_credit_card(credit_df):
    try:
        credit_df = credit_df.withColumnRenamed("CREDIT_CARD_NO", "CUST_CC_NO")
        
        credit_df = credit_df.withColumn("TIMEID", 
                                         concat(
                                             col("YEAR"), 
                                             lpad(col("MONTH").cast("string"), 2, "0"), 
                                             lpad(col("DAY").cast("string"), 2, "0")
                                         ).cast("string"))
        
        # Log successful transformation
        logging.info(f"Data transformation for credit card transactions successful at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        return credit_df
    
    except Exception as e:
        logging.error(f"Error occurred in transform_credit_card function: {str(e)}")
        print(f"Error occurred: {str(e)}")
        raise

# Function that creates a new database
def create_database(host, user, password, database_name):
    try:
        # Establish a connection to the MySQL server
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password
        )
        
        # Check if the connection is established
        if connection.is_connected():
            cursor = connection.cursor()  # Create a cursor object to interact with the database
             # Log MYSQL connection established
            logging.info(f"MySQL connection is established at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
            # Check if the database already exists
            cursor.execute(f"SHOW DATABASES LIKE '{database_name}'")
            database_exists = cursor.fetchone()

            if not database_exists:
                # If the database doesn't exist, create it
                cursor.execute(f"CREATE DATABASE {database_name}")
                # Log successful database creation
                logging.info(f"Database '{database_name}' created successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                 # Log database already exists
                logging.info(f"Database '{database_name}' already exists at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    except Error as e:
        print(f"Error: '{e}'")  # Print any errors that occur during the connection or execution
    
    finally:
        if connection.is_connected():
            cursor.close()  # Close the cursor
            connection.close()  # Close the connection to the MySQL server

             # Log closing MYSQL connection
            logging.info(f"MySQL connection is closed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
# Function that loads data to db: creditcard_capstone
def load_to_mysql(dataframe, table_name):
    try:
        dataframe.write.format("jdbc") \
            .mode("overwrite") \
            .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
            .option("dbtable", table_name) \
            .option("user", config.user) \
            .option("password", config.password) \
            .save()

        # Logging successful loading with timestamp
        logging.info("Data loaded into MySQL table %s successful at %s.", table_name, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        
    except Exception as e:
        logging.error(f"Error occurred in load_to_mysql function: {str(e)}")  # logging of error to etl.log
        print(f"Error occurred: {str(e)}")  #  user-friendly print statement for error
        raise


#Function for ETL
def etl_data(spark, extract_func, transform_func, json_file, table_name):
    try:
        # Extract data
        extracted_df = extract_func(spark, json_file)
        
        # Transform data
        transformed_df = transform_func(extracted_df)
        
        # Load data to MySQL using load_to_mysql function
        load_to_mysql(transformed_df, table_name)
        
        # Print success message
        print(f"{table_name} - ETL Successful!")
        
    except Exception as e:
        logging.error(f"Error occurred in ETL process for {table_name}: {str(e)}")
        print(f"Error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        # Create database
        database_name = "creditcard_capstone"
        create_database("localhost", config.user, config.password, database_name)
        
        # Creating Spark Session
        spark = SparkSession.builder \
            .appName('ETL') \
            .config("spark.logConf", "true") \
            .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=log4j.properties") \
            .config("spark.sql.debug.maxToStringFields", 1000) \
            .getOrCreate()
        
        # Set log level to ERROR
        spark.sparkContext.setLogLevel("ERROR")
        
        # ETL Branch data
        etl_data(spark, extract_branch, transform_branch, "data/cdw_sapp_branch.json", "CDW_SAPP_BRANCH")
        
        # ETL Credit Card Transaction data
        etl_data(spark, extract_credit_card, transform_credit_card, "data/cdw_sapp_credit.json", "CDW_SAPP_CREDIT_CARD")
        
        # ETL Customer data
        etl_data(spark, extract_customer, transform_customer, "data/cdw_sapp_customer.json", "CDW_SAPP_CUSTOMER")
        
    except Exception as e:
        logging.error(f"Error occurred in ETL's main function: {str(e)}")
        print(f"Error occurred: {str(e)}")