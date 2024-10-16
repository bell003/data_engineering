import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lag, col, when, sum, first, last, to_date
from pyspark.sql.types import StructType, StringType, DateType, StructField
from pyspark.sql.window import Window

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("first_end_date").getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("INFO")

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

df = spark.createDataFrame(data,["event_date","event_status"])

df_new = df.withColumn("event_date", to_date(col("event_date")))

window_spec_lag = Window.orderBy("event_date")

df_1 = df.withColumn("event_change",when(col("event_status") != lag("event_status").over(window_spec_lag),1).otherwise(0))

df_2 = df_1.withColumn("event_change",col("event_change").cast("int"))

window_spec_sum = Window.orderBy("event_date").rowsBetween(Window.unboundedPreceding,Window.currentRow)

df_3 = df_2.withColumn("event_group", sum("event_change").over(window_spec_sum))

df_3.show()

output = df_3.groupBy("event_group", "event_status") \
    .agg(
        first("event_date").alias("start_date"),
        last("event_date").alias("end_date")
    ) \
    .drop("event_group")

# Show the result
output.show()