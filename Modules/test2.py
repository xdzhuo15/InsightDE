from freqencoder import FreqEncoder, FreqEncoderModel
from pyspark.sql import SQLContext, SparkSession
from pyspark.ml.feature import VectorAssembler
import pyspark
from pyspark import SparkConf
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression

spark = SparkSession.builder.appName("ReadData").getOrCreate()

data = [
    ("Tax", 2.0),
    ("Tax", 10.2),
    ("Food", 13.2),
    ("Moon", 5.4),
    ("Tax", 8.6),
    ("Moon", 7.5),
    ("Wind", 1.7)
]

df = spark.createDataFrame(data, ["cat", "num"])

indexer = FreqEncoder().setInputCol("cat").setOutputCol("count_cat")
model1 = indexer.fit(df)
output1 = model1.transform(df)
output1.show()
print "showing original dataframe"
df.show()

lr = LinearRegression(featuresCol = "features_vec", labelCol="HasDetections",maxIter=10 )
#assembler = VectorAssembler(inputCols =["count_cat","num"], outputCol="feature")
#model2 = assembler.fit(output1)
#output2 = model2.transform(output1)

#output2.show()
#print "showing original dataframe"
#df.show()

stages = []
indexer = FreqEncoder().setInputCol("cat").setOutputCol("count_cat")
stages += [indexer]
assembler = VectorAssembler(inputCols=["count_cat","num"],outputCol="feature")
stages += [assembler]
pipeline = Pipeline(stages = stages)

model = pipeline.fit(df)
features =model.transform(df)

features.show()
