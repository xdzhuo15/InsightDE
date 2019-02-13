import custom_module import FreqEncoder, FreqEncoderModel
from pyspark.sql import SQLContext, SparkSession
from pyspark.ml.feature import Imputer, VectorAssembler, MinMaxScaler, StringIndexer
import pyspark
from pyspark import SparkConf

conf = SparkConf()
conf.set("spark.executor.memory", "1g")
conf.set("spark.cores.max", "2")
conf.set("spark.jars", 'spark-mllib-custom-models-assembly-0.1.jar')
conf.set("spark.app.name", "sparkTestApp")

spark = pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()

#spark = SparkSession.builder.appName("ReadData").getOrCreate()

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
