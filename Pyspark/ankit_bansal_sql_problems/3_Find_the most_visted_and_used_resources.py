import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, rank, collect_list, concat_ws
from pyspark.sql.window import Window

os.environ['PYSPARK_PYTHON']=sys.executable
os.environ['PYSPARK_DRIVER_PYTHON']=sys.executable

# Initialize Spark session
spark = SparkSession.builder.appName("EntriesApp").getOrCreate()

# Create the DataFrame
data = [
    ('A', 'Bangalore', 'A@gmail.com', 1, 'CPU'),
    ('A', 'Bangalore', 'A1@gmail.com', 1, 'CPU'),
    ('A', 'Bangalore', 'A2@gmail.com', 2, 'DESKTOP'),
    ('B', 'Bangalore', 'B@gmail.com', 2, 'DESKTOP'),
    ('B', 'Bangalore', 'B1@gmail.com', 2, 'DESKTOP'),
    ('B', 'Bangalore', 'B2@gmail.com', 1, 'MONITOR')
]

columns = ['name', 'address', 'email', 'floor', 'resources']

df = spark.createDataFrame(data, columns)

# Window specification for ranking
window_spec = Window.partitionBy("name").orderBy(col("count").desc())

# Get the most visited floor and used resources in a single transformation
result = (df.groupBy("name", "floor")
            .agg(count("*").alias("count"))
            .withColumn("rank", rank().over(window_spec))
            .filter(col("rank") == 1)
            .drop("count", "rank")
            .join(df.groupBy("name").agg(concat_ws(",", collect_list("resources")).alias("Used_resources")), "name")
            .select("name", col("floor").alias("most_visited"), "Used_resources"))

# Show the result
result.show(truncate=False)
