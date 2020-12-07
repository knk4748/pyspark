
from pyspark.sql import SparkSession

logFile = "word_count.text"  # Should be some file on your system
spark = SparkSession.builder.appName("DocNotes1").getOrCreate()
logData = spark.read.text(logFile)

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()
num = logData.count() #counting lines
print("Lines with a: %i, lines with b: %i ,count %i" % (numAs, numBs,num))

spark.stop()