# Import needed libraries
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import initcap, lower, concat, col, regexp_replace, lit, substring, lpad

# Create Spark Session
spark = SparkSession.builder.appName('Customer').getOrCreate()

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

# Create Spark df and Reading/loading the Dataset from json file 
cust_df = spark.read.option("multiline", "true").schema(schema).json("data/cdw_sapp_customer.json")

# Convert the "FIRST_NAME" column to Title Case
cust_df = cust_df.withColumn("FIRST_NAME", initcap(cust_df["FIRST_NAME"]))
# Convert the "MIDDLE_NAME" column to lower Case
cust_df = cust_df.withColumn("MIDDLE_NAME", lower(cust_df["MIDDLE_NAME"]))
# Convert the "LAST_NAME" column to Title Case
cust_df = cust_df.withColumn("LAST_NAME", initcap(cust_df["LAST_NAME"]))

# Create new column "FULL_STREET_ADDRESS" by concatenating "APT_NO" and "STREET_NAME"
cust_df = cust_df.withColumn("FULL_STREET_ADDRESS", concat(col("APT_NO"), lit(", "), col("STREET_NAME")).cast(StringType()))

#Since the phone number is only 7 digits, pad it on the left with 000 as the area code
cust_df = cust_df.withColumn("CUST_PHONE", lpad(regexp_replace(col("CUST_PHONE"), "[^0-9]", ""), 10, '000'))

# Change the format of phone number to (XXX)XXX-XXXX
cust_df = cust_df.withColumn("CUST_PHONE", 
                             concat(lit("("), substring("CUST_PHONE", 1, 3), lit(")"),
                                    substring("CUST_PHONE", 4, 3), lit("-"),
                                    substring("CUST_PHONE", 7, 4)))

print("Customer Data Successfully Cleaned")

# # Show the updated DataFrame
# cust_df.select("CUST_PHONE").show(5)

# # Show the updated DataFrame
# cust_df.show(5)
