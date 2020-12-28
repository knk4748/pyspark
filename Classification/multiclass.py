from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkConf, SparkContext
from numpy import array
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
import pandas as pd
from pyspark.ml.classification import LogisticRegression, OneVsRest
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from sklearn.preprocessing import LabelEncoder


spark = SparkSession.builder.master('local[*]').getOrCreate()

dataset = spark.read.csv("mushrooms.csv",header=True,inferSchema=True)
print("size of the data set: ", dataset.count(), len(dataset.columns))
dataset.show()


cols=dataset.columns
data = dataset.select("*").toPandas()

df2 = pd.get_dummies(data, prefix=cols)
cols = df2.columns


habitats = ["habitat_d","habitat_g","habitat_l","habitat_m","habitat_p","habitat_u","habitat_w"]
print("total :",len(cols))
print("habitat :",type(habitats))

print(len(habitats))
inputCols =[]
for c in cols:
    if c not in habitats:
        inputCols.append(c)

print("input ", len(inputCols))

pdf = pd.DataFrame(df2)
df = spark.createDataFrame(pdf)
df.show()

featureassembler=VectorAssembler(inputCols=inputCols,outputCol="features")
XY = featureassembler.transform(df).select("features","habitat_d","habitat_g","habitat_l","habitat_m","habitat_p","habitat_u","habitat_w")
XY.show()
train,test = XY.randomSplit([.8,.2])

accuracyList = []
for habitat in habitats:
    _train = train.withColumnRenamed(habitat,"label")
    _test = test.withColumnRenamed(habitat,"label").select("features","label")
    lr = LogisticRegression(maxIter=30, tol=1E-6, fitIntercept=True)
    ovr = OneVsRest(classifier=lr)
    ovrModel = ovr.fit(_train)
    predictions = ovrModel.transform(_test)
    evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    accuracyList.append(accuracy)
    print("Test Error = %g" % (1.0 - accuracy))
    
    
 



