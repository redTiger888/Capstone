#import libraries
import warnings

import requests
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import config  # Contains user and password for MySQL

# Suppress all warnings
warnings.filterwarnings("ignore")

logging.basicConfig(filename='loan_application.log', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

# Function to fetch data from the API endpoint
def fetch_loan_data():
    url = 'https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json'
    try:
        response = requests.get(url)
        logging.info(f"Status code: {response.status_code}")
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"Failed to fetch data. Status code: {response.status_code}")
            return None
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        return None

# Function to create a Spark session
def create_spark_session(app_name='LoanApplicationData'):
    return SparkSession.builder.appName(app_name).getOrCreate()

# Function to load data into MySQL database using PySpark
def load_to_mysql(data):
    try:
        # Create PySpark session
        spark = create_spark_session()

        if not spark:
            return

        # Define schema based on sample data
        schema = StructType([
            StructField("Application_ID", StringType(), True),
            StructField("Gender", StringType(), True),
            StructField("Married", StringType(), True),
            StructField("Dependents", StringType(), True),
            StructField("Education", StringType(), True),
            StructField("Self_Employed", StringType(), True),
            StructField("Credit_History", IntegerType(), True),
            StructField("Property_Area", StringType(), True),
            StructField("Income", StringType(), True),
            StructField("Application_Status", StringType(), True)
        ])

        # Create DataFrame
        df = spark.createDataFrame(data, schema=schema)

        # Write DataFrame to MySQL
        df.write.format('jdbc').options(
            url='jdbc:mysql://localhost:3306/creditcard_capstone',
            driver='com.mysql.cj.jdbc.Driver',
            dbtable='CDW_SAPP_loan_application',
            user=config.user,
            password=config.password
        ).mode('append').save()

        logging.info("Data loaded successfully to MySQL database.")
    except Exception as e:
        logging.error(f"Error loading data to MySQL: {e}")

# Main function to orchestrate the loan application process
def main():
    logging.info("Starting the loan application data fetch and load process.")
    
    # Fetch loan application data from the API
    loan_data = fetch_loan_data()

    # If data is fetched successfully, load it into MySQL database
    if loan_data:
        load_to_mysql(loan_data)
    else:
        logging.error("No data fetched from the API.")

if __name__ == "__main__":
    main()
