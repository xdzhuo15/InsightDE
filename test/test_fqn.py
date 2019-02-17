from freqencoder import FreqEncoder, FreqEncoderModel
from pyspark.sql import SQLContext, SparkSession
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
import pyspark
from pyspark import SparkConf
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression
from pyspark.sql.types import *

spark = SparkSession.builder.appName("ReadData").getOrCreate()

Schema = StructType([
          StructField('Cat', StringType()),
          StructField('Num', DoubleType()),
          StructField('Label', DoubleType())
          ])

df = spark.read.csv("data_fq.csv", header = True, schema=Schema)
df.show()

encoder = FreqEncoder().setInputCol("Cat").setOutputCol("Count_cat")
model1 = encoder.fit(df)
output1 = model1.transform(df)
output1.show()

stages = []
indexer = FreqEncoder().setInputCol("cat").setOutputCol("count_cat")
stages += [indexer]
assembler = VectorAssembler(inputCols=["count_cat","num"],outputCol="feature")
stages += [assembler]
lr = LinearRegression(featuresCol = "feature", labelCol="label", maxIter=3 )
stages += [lr]
pipeline = Pipeline(stages = stages)

model2 = pipeline.fit(df)
prediction = model2.transform(df)

print "showing prediction"
prediction.show()
print "showing original dataframe"
df.show()
