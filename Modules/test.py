from freqencoder import FreqEncoder, FreqEncoderModel
from pyspark.sql import SQLContext, SparkSession
from pyspark.ml.feature import StringIndexer
import pyspark
from pyspark import SparkConf


spark = SparkSession.builder.appName("ReadData").getOrCreate()

data = [
    ("Tax", 3),
    ("Tax", 3),
    ("Food", 1),
    ("Moon", 2),
    ("Tax", 3),
    ("Moon", 2),
    ("10.0", 1)
]

df = spark.createDataFrame(data, ["input", "expected"])

indexer = StringIndexer().setInputCol("input").setOutputCol("new_index")
data = indexer.fit(df)

data.transform(df).show()

freqencoder = FreqEncoder() \
    .setInputCol("input") \
    .setOutputCol("bin")

model = freqencoder.fit(df)

model.transform(df).show()
