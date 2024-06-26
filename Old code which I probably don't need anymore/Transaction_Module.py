# Importing the Libraries
import warnings
# Suppress all warnings
warnings.filterwarnings("ignore")
import logging

# Suppress warning messages from NativeCodeLoader
logging.getLogger("org.apache.hadoop.util.NativeCodeLoader").setLevel(logging.ERROR)

import pyspark
from pyspark.sql import SparkSession
import config

# Function to Prompt user to enter Zip code and verify format
def get_zipcode():
    while True:
        zip_code = input("Enter zip code (for example: 01824)").strip()

        # Verify zip code has 5 digits
        if zip_code.isdigit() and len(zip_code) == 5:
            return zip_code
        else:
            print("Invalid zip code entered. Please enter a 5-digit numeric zip code.")

# Function to Prompt user to enter Month and Year and check validity
def get_month_year():
    while True:
        month = input("Enter Month (1-12): ").strip()
        year = input("Enter Year (for example: 2024): ").strip()
        
        # Verify month is between 1 & 12 and year has 4 digits
        if (month.isdigit() and 1 <= int(month) <= 12) and (year.isdigit() and len(year) == 4):
            return int(month), int(year)
        else:
            print("Invalid input. Please enter a valid month (1-12) and 4-digit numeric year.")

#Function to make a query the sql database based on the user entered zip code, month and year
def query_transactions(zip_code, month, year, spark):
    #JDBC parameters
    jdbc_url = "jdbc:mysql://localhost:3306/creditcard_capstone"
    
    dbtable_query = f'''(SELECT timeid AS Date, TRANSACTION_TYPE AS Type, 
                    TRANSACTION_VALUE AS Amount, cust.FIRST_NAME AS "First Name", cust.LAST_NAME AS "Last Name"
                    FROM cdw_sapp_credit_card cc
                    JOIN cdw_sapp_customer cust ON cust.CREDIT_CARD_NO = cc.CUST_CC_NO
                    WHERE cc.month = {month} AND cc.year = {year} AND cust_zip = {zip_code}
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

        return df

    except Exception as e:
        print(f"Error querying data: {str(e)}")
        return None

#The master function that pulls all the transaction details by calling the above functions
def get_transaction_detail(spark):
    zip_code = get_zipcode()
    month, year = get_month_year()
    df = query_transactions(zip_code, month, year, spark)

    if df is not None:
        df.show(df.count(), truncate=False)
    else:
        print("No data retrieved or error occurred.")

   
if __name__ == "__main__":
    # Creating Spark Session
    # spark = SparkSession.builder.appName('Query Transactions').getOrCreate()
    spark = SparkSession.builder \
    .appName('Query Transactions') \
    .config('spark.eventLog.gcMetrics.youngGenerationGarbageCollectors', 'G1 Young Generation') \
    .config('spark.eventLog.gcMetrics.oldGenerationGarbageCollectors', 'G1 Old Generation') \
    .config('spark.executor.extraJavaOptions', '-Djava.library.path=/path/to/native/libraries') \
    .getOrCreate()

    get_transaction_detail(spark)

    # Stop Spark session after usage
    spark.stop()

