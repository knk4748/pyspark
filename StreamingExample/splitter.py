from pyspark.sql import SparkSession
import pyspark.sql.functions 
import pyspark.sql.types 

spark = SparkSession.builder.master('local[*]').getOrCreate()
df = spark.read.csv("PS_20174392719_1491204439457_log.csv", header=True, inferSchema=True)
df = df.drop("isFraud", "isFlaggedFraud")

print("size of the data set: ", df.count(), len(df.columns))


steps = df.select("step").distinct().collect()
print("first 3 lines",steps[:3])


steps = df.select("step").distinct().collect()
for step in steps[:]:
   _df = df.where(f"step = {step[0]}")
   _df.coalesce(1).write.mode("append").option("header", "true").csv("data/paysim")
   print("one more!")

