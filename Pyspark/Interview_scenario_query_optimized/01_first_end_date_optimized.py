import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lag, col, when, sum, first, last, to_date
from pyspark.sql.window import Window

# Setting up the environment variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Initialize Spark session
spark = SparkSession.builder.appName("first_end_date").getOrCreate()

# Suppress excessive logging
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Sample data
data = [
    ("2020-06-01", "won"),
    ("2020-06-01", "won"),
    ("2020-06-02", "won"),
    ("2020-06-03", "won"),
    ("2020-06-04", "lost"),
    ("2020-06-05", "lost"),
    ("2020-06-06", "lost"),
    ("2020-06-07", "won"),
    ("2020-06-08", "won"),
    ("2020-06-09", "lost"),
    ("2020-06-09", "lost")
]

# Create DataFrame
df = spark.createDataFrame(data, ["event_date", "event_status"])

# Convert 'event_date' to DateType
df_new = df.withColumn("event_date", to_date(col("event_date")))

# Define window specification for lag
window_spec_lag = Window.orderBy("event_date")

# Chain operations: identify event changes and calculate the cumulative sum
window_spec_sum = Window.orderBy("event_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_final = df_new.withColumn(
    "event_change", when(col("event_status") != lag("event_status").over(window_spec_lag), 1).otherwise(0)
).withColumn(
    "event_group", sum("event_change").over(window_spec_sum)
)

# Perform aggregation to find start and end date for each event group
output = df_final.groupBy("event_group", "event_status") \
    .agg(
        first("event_date").alias("start_date"),
        last("event_date").alias("end_date")
    ) \
    .drop("event_group")

# Show the final result
output.show()
