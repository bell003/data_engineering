from pyspark.sql import SparkSession
from pyspark.sql.functions import lag, col, lead, when
from pyspark.sql.window import Window
import os
import sys

# Setting environment variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Initialize Spark session
spark = SparkSession.builder.appName("first_end_date").getOrCreate()

# Suppress excessive logging
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Sample data and schema
data = [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "David"), (5, "Eve")]
schema = ['id', 'Name']

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Window specification
window_spec = Window.orderBy("id")

# Create 'pre_value' and 'next_value' columns using lag and lead
df_1 = df.withColumn("pre_value", lag("Name").over(window_spec))\
         .withColumn("Next_value", lead("Name").over(window_spec))

# Add 'New_column' based on id being even or odd
df_2 = df_1.withColumn("New_column", when(col('id') % 2 == 0, col("pre_value")).otherwise(col("Next_value")))

# Handle nulls in 'New_column' and drop unwanted columns
df_3 = df_2.withColumn(
    "New_column", when(col("New_column").isNull(), col("Name")).otherwise(col("New_column"))
).drop("Name", "pre_value", "Next_value").withColumnRenamed("New_column", "Name")

# Show the final DataFrame
df_3.show()
