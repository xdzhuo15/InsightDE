from freqencoder import FreqEncoder, FreqEncoderModel
from pyspark.sql import SQLContext, SparkSession
from pyspark.ml.feature import VectorAssembler
import pyspark
from pyspark import SparkConf
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression

spark = SparkSession.builder.appName("ReadData").getOrCreate()

data = [
    ("Tax", 15.0, 3.7),
    ("Tax", 10.2, 4.5),
    ("Food", 3.2, 2),
    ("Moon", 5.4, 1.7),
    ("Tax", 8.6, 5.3),
    ("Moon", 7.5, 3.0),
    ("Wind", 1.7, 2.4)
]

df = spark.createDataFrame(data, ["cat", "num","label"])

encoder = FreqEncoder().setInputCol("cat").setOutputCol("count_cat")
model1 = encoder.fit(df)
output1 = model1.transform(df)
output1.show()
df.show()

stages = []
indexer = FreqEncoder().setInputCol("cat").setOutputCol("count_cat")
stages += [indexer]
assembler = VectorAssembler(inputCols=["count_cat","num"],outputCol="feature")
stages += [assembler]
lr = LinearRegression(featuresCol = "feature", labelCol="result",maxIter=3 )
pipeline = Pipeline(stages = stages)

model2 = pipeline.fit(df)
prediction = model2.transform(df)

print "showing prediction"
prediction.show()
print "showing original dataframe"
df.show()

#assembler = VectorAssembler(inputCols =["count_cat","num"], outputCol="feature")
#model2 = assembler.fit(output1)
#output2 = model2.transform(output1)

#output2.show()
#print "showing original dataframe"
#df.show()
