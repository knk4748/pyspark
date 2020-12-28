from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkConf, SparkContext
from numpy import array
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler


spark = SparkSession.builder.master('local[*]').getOrCreate()

dataset = spark.read.csv("mushrooms.csv",header=True,inferSchema=True)
print("size of the data set: ", dataset.count(), len(dataset.columns))
dataset.show()


from sklearn.preprocessing import LabelEncoder
data = dataset.select("*").toPandas()
k = data.keys()
for i in k:
    le = LabelEncoder()
    if str(i)=="class":
        n = "label"
    else:
        n = str(i) + "_n"
    data[n] = le.fit_transform(data[i])

for i in k:
    del data[i]
data.head()


df = spark.createDataFrame(data)
df.show()

input_cols = ['cap-shape_n', 'cap-surface_n', 'cap-color_n', 'bruises_n',
       'odor_n', 'gill-attachment_n', 'gill-spacing_n', 'gill-size_n',
       'gill-color_n', 'stalk-shape_n', 'stalk-root_n',
       'stalk-surface-above-ring_n', 'stalk-surface-below-ring_n',
       'stalk-color-above-ring_n', 'stalk-color-below-ring_n', 'veil-type_n',
       'veil-color_n', 'ring-number_n', 'ring-type_n', 'spore-print-color_n',
       'population_n', 'habitat_n']

featureassembler=VectorAssembler(inputCols=input_cols,outputCol="features")

XY = featureassembler.transform(df).select("features","label")
XY.show()


train,test = XY.randomSplit([.8,.2])

from pyspark.ml.classification import LogisticRegression, OneVsRest
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

lr = LogisticRegression(maxIter=30, tol=1E-6, fitIntercept=True)

ovr = OneVsRest(classifier=lr)

ovrModel = ovr.fit(train)

predictions = ovrModel.transform(test)

evaluator = MulticlassClassificationEvaluator(metricName="accuracy")

accuracy = evaluator.evaluate(predictions)
print("Test Error = %g" % (1.0 - accuracy))




















