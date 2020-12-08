from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types 

spark = SparkSession.builder.master('local[*]').getOrCreate()
df = spark.read.csv("PS_20174392719_1491204439457_log.csv", header=True, inferSchema=True)
dataSchema = df.schema



streaming = (
    spark.readStream.schema(dataSchema)
    .option("maxFilesPerTrigger", 1)
    .csv("data/paysim/")
)
print("Debug 1")
dest_count = streaming.groupBy("nameDest").count().orderBy(F.desc("count"))
print("Debug 2")
activityQuery = (
    dest_count.writeStream.queryName("dest_counts")
    .format("memory")
    .outputMode("complete")
    .start()
)
print("Debug 3")
# activityQuery.awaitTermination()
# I tried using it but it didn't worked 
import time
print("Debug 4")
for x in range(50):
    _df = spark.sql(
        "SELECT * FROM dest_counts WHERE nameDest != 'nameDest' AND count >= 2"
    )
    if _df.count() > 0:
        _df.show(20)
    time.sleep(1)

    print("Streaming: ",_df.count())