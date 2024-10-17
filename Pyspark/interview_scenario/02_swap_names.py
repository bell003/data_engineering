from pyspark.sql import SparkSession
from pyspark.sql.functions import lag, col, lead, when, coalesce
from pyspark.sql.window import Window
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("first_end_date").getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

data = [(1,"Alice"),(2,"Bob"),(3,"Charlie"),(4,"David"),(5,"Eve")]
schema = ['id','Name']

df = spark.createDataFrame(data,schema)

window_spec = Window.orderBy("id")

df_1 = df.withColumn("pre_value",lag("Name").over(window_spec)).withColumn("Next_value", lead("Name").over(window_spec))

df_2 = df_1.withColumn("New_column", when(col('id') % 2==0,col("pre_value")).otherwise(col("Next_value")))

df_2.withColumn(
    "new_column", when(col("new_column").isNull(), col("Name")).otherwise(col("new_column"))
).drop("Name","Pre_value","Next_value").withColumnRenamed("new_column","Name").show()

