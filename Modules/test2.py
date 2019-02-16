from freqencoder import FreqEncoder, FreqEncoderModel
from pyspark.sql import SQLContext, SparkSession
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
import pyspark
from pyspark import SparkConf
from pyspark.ml import Pipeline


spark = SparkSession.builder.appName("ReadData").getOrCreate()

data = [
    ("Tax", 2),
    ("Tax", 10),
    ("Food", 13),
    ("Moon", 5),
    ("Tax", 8),
    ("Moon", 7),
    ("Wind", 1)
]

df = spark.createDataFrame(data, ["cat", "num"])

stages = []
indexer = FreqEncoder().setInputCol("cat").setOutputCol("count_cat")
stages += [indexer]
for col in ["num","count_cat"]:
     scaler = MinMaxScaler().setInputCol(col).setOutputCol(col+"_norm")
     stages += [stages]
pipeline = Pipeline(stages = stages)

model = pipeline.fit(df)
features =model.transform(df)

features.show()
