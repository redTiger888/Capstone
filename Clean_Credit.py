# Importing the Libraries
import warnings

# Suppress all warnings
warnings.filterwarnings("ignore")

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import concat, col, lpad

# Creating Spark Session
spark = SparkSession.builder.appName('Credit Card Transaction').getOrCreate()

schema = StructType([
    StructField("TRANSACTION_ID", IntegerType(), True),
    StructField("DAY", IntegerType(), True),
    StructField("MONTH", IntegerType(), True),
    StructField("YEAR", IntegerType(), True),
    StructField("TIMEID", StringType(), True),
    StructField("CUST_CC_NO", StringType(), True),
    StructField("CUST_SSN", IntegerType(), True),
    StructField("BRANCH_CODE", IntegerType(), True),
    StructField("TRANSACTION_TYPE", StringType(), True),
    StructField("TRANSACTION_VALUE", DoubleType(), True)
])

# Create SparkDataFrame and Reading/loading the Dataset from json file 
credit_df = spark.read.option("multiline", "true").schema(schema).json("data/cdw_sapp_credit.json")

# Rename the "CREDIT_CARD_NO" column to "CUST_CC_NO"
credit_df = credit_df.withColumnRenamed("CREDIT_CARD_NO", "CUST_CC_NO")

# Convert DAY, MONTH, and YEAR into a new column namec TIMEID (YYYYMMDD)
credit_df = credit_df.withColumn("TIMEID", 
                                 concat(
                                     col("YEAR"), 
                                     lpad(col("MONTH").cast("string"), 2, "0"), 
                                     lpad(col("DAY").cast("string"), 2, "0")
                                 ).cast("string"))

print("Credit Card Transactions, cleaned successfully!")

credit_df.show(5)