from pyspark.sql import SparkSession
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler



spark = SparkSession.builder.appName('Ecomm').getOrCreate()
dataset = spark.read.csv("Ecomm.csv",header=True,inferSchema=True)
dataset.printSchema()

featureassembler=VectorAssembler(inputCols=["Avg Session Length","Time on App","Time on Website","Length of Membership"],outputCol="Independent Features")
XY = featureassembler.transform(dataset).select("Independent Features","Yearly Amount Spent")


train,test = XY.randomSplit([.8,.2])

regressor = RandomForestRegressor(featuresCol="Independent Features",labelCol='Yearly Amount Spent')
model = regressor.fit(train)


result = model.transform(test)
result.show()
