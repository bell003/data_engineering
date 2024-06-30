import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, date_add, dayofweek

os.environ['PYSPARK_PYTHON']=sys.executable
os.environ['PYSPARK_DRIVER_PYTHON']=sys.executable

# Initialize SparkSession
spark = SparkSession.builder.appName("DateManipulation").getOrCreate()

# Define the variables
today_date = '2024-06-30'
n = 3

# Create a DataFrame with the today_date value
df = spark.createDataFrame([(today_date,)], ["today_date"])

# Calculate the result date
# First, get the day of the week for today_date (Sunday = 1, ..., Saturday = 7)
df = df.withColumn("day_of_week", dayofweek(col("today_date")))

# Calculate the days to add to reach the next Monday
df = df.withColumn("days_to_add", expr("8 - day_of_week"))

# Add the necessary days to today_date
df = df.withColumn("next_monday", date_add(col("today_date"), col("days_to_add")))

# Add (n-1) weeks to the next Monday
df = df.withColumn("result_date", date_add(col("next_monday"), (n-1) * 7))

# Show the result
df.select("result_date").show()
