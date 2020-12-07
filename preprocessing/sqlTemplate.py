from pyspark.sql import SparkSession
from pyspark.sql import Row

logFile = "BankChurnersModified.csv"
spark = SparkSession.builder.appName("sqlNote").getOrCreate()

df = spark.read.csv(logFile,header=True)
df.show()
print(df.first())

df.createOrReplaceTempView("CLIENTS")


teenagers = spark.sql("SELECT CLIENTNUM,Months_on_book,Marital_Status FROM CLIENTS WHERE Months_on_book >= 40 a")



print()
print()
print()
print()

teenagers.show()



df.printSchema()
spark.stop()