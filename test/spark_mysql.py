from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession

spark = SparkSession.builder.appName("ReadData").getOrCreate()

df = spark.read.format("jdbc").options(
    url="jdbc:MYSQL_ADRESS:PORT/Prediction",
    driver = "com.mysql.cj.jdbc.Driver",
    dbtable = "R",
    user = "USERNAME",
    password = "PASSWORD").load()
df.show()
