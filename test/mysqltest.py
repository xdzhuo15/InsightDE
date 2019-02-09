from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession

spark = SparkSession.builder.appName('ReadData').getOrCreate()

df = spark.read.format("jdbc").options(
    url='jdbc:mysql://ec2-34-211-3-37.us-west-2.compute.amazonaws.com:3306/Prediction',
    driver = 'com.mysql.cj.jdbc.Driver',
    dbtable = 'R',
    user = 'USER',
    password = 'PASSWORD').load()
df.show()
