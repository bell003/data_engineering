from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id

import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("count_each_column").getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

data = [
 (10,20,11,20),
 (20, 11, 10,99),
 (10, 11, 20,  1),
 (30, 12, 20,99),
 (10, 11, 20, 20),
 (40, 13, 15,  3),
 (30, 8, 11, 99)
]
schema = "A int , B int , C int , D int"
df = spark.createDataFrame(data = data , schema = schema)

df.show()

df_list = []

for col in df.columns:
    df_col = df.select(col).groupBy(col).count()
    df_col = df_col.withColumnRenamed('count',f"count_{col}")
    df_col.show()
    df_list.append(df_col)

df_list = [df.withColumn("index", monotonically_increasing_id()) for df in df_list]

print(df_list)

df_final = df_list[0]

for df in df_list[1:]:
    df_final = df_final.join(df,on='index',how='outer')

df_final.show()

df_final = df_final.drop("index")

df_final.show()