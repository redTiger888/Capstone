# Importing the Libraries
import pyspark
from pyspark.sql import SparkSession

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import concat, col, lit, substring, lpad, when, length

# Creating Spark Session
spark = SparkSession.builder.appName('Branch').getOrCreate()

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

# Create SparkDataFrame and Reading/loading the Dataset from json file 
branch_df = spark.read.option("multiline", "true").schema(schema).json("data/cdw_sapp_branch.json")


# Change the format of phone number to (XXX)XXX-XXXX
branch_df = branch_df.withColumn("BRANCH_PHONE", 
                                 concat(lit("("), substring("BRANCH_PHONE", 1, 3), lit(")"),
                                    substring("BRANCH_PHONE", 4, 3), lit("-"),
                                    substring("BRANCH_PHONE", 7, 4)))

#Add logic to handle Zip code
# if null, make it 99999; if 4 digit, add left pad with 0
branch_df = branch_df.withColumn("BRANCH_ZIP", 
                                 when(branch_df["BRANCH_ZIP"].isNull(), "99999")  # Replace null values with "99999"
                                .otherwise(                                   # Otherwise, check if leading zero is needed
                                    when(length(col("BRANCH_ZIP")) == 4, lpad(col("BRANCH_ZIP"), 5, "0"))  
                                    .otherwise(col("BRANCH_ZIP"))))

print("Branch Data Successfully Cleaned")

# Show the updated DataFrame
branch_df.select("BRANCH_PHONE").show(5)

# Show the updated DataFrame
branch_df.show(5)
