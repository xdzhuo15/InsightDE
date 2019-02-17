from freqencoder import FreqEncoder, FreqEncoderModel
from pyspark.sql import SQLContext, SparkSession
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
import pyspark
from pyspark import SparkConf
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression

spark = SparkSession.builder.appName("ReadData").getOrCreate()

df = spark.read.csv("data_fq.csv", header = True)
df.show()

encoder = FreqEncoder().setInputCol("Cat").setOutputCol("Count_cat")
model1 = encoder.fit(df)
output1 = model1.transform(df)
output1.show()

stages = []
indexer = FreqEncoder().setInputCol("Cat").setOutputCol("Count_cat")
stages += [indexer]
assembler = VectorAssembler(inputCols=["Count_cat","Num"],outputCol="Features")
stages += [assembler]
lr = LinearRegression(featuresCol = "Features", labelCol="Label", maxIter=3 )
stages += [lr]
pipeline = Pipeline(stages = stages)

model2 = pipeline.fit(df)
prediction = model2.transform(df)

prediction.show()
df.show()

#assembler = VectorAssembler(inputCols =["count_cat","num"], outputCol="feature")
#model2 = assembler.fit(output1)
#output2 = model2.transform(output1)

#output2.show()
#print "showing original dataframe"
#df.show()
