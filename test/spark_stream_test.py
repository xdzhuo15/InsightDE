#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 30 12:07:24 2019

@author: xdzhuo
"""

from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import os
from pyspark import SparkConf
from pyspark.sql import SQLContext, SparkSession, Row, Column
from pyspark.sql.types import *
import json
from test_schema import Schema
conf = SparkConf().setAppName("prediction").setMaster(
        "spark://ec2-52-10-44-193.us-west-2.compute.amazonaws.com:7077"
        )

sc = SparkContext(conf=conf)
#sc.setLogLevel("WARN")

ssc = StreamingContext(sc, 5)

kafka_stream = KafkaUtils.createDirectStream(ssc, 
    ["DeviceRecord"], {"metadata.broker.list":"ip-10-0-0-7:9092,ip-10-0-0-11:9092,ip-10-0-0-10:9092"})
#lines = kafka_stream.map(lambda x: x[1])

print 'Event recieved in window!!!!!!: ', kafka_stream.pprint()

#df = sc.createDataFrame(kafka_stream)

#lines.select('AvSigVersion','IsBeta','CityIdentifier').shof
#kafka_stream = ssc.union(kafka_stream)
#kafka_stream = kafka_stream.map(lambda x: x.decode("utf-8"))
#kafka_stream.pprint()
#parsed = kafka_stream.map(lambda x: json.loads(x[1]))
#parsed.printSchema()

#kafka_stream.foreachrdd
#sqlContext = SQLContext.getOrCreate(kafka_stream.context)

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']

def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        # Convert RDD[String] to RDD[Row] to DataFrame
        rowRdd = rdd.map(lambda w: Row(word=w))
        wordsDataFrame = spark.createDataFrame(rowRdd)

        # Creates a temporary view using the DataFrame
        wordsDataFrame.createOrReplaceTempView("words")

        # Do word count on table using SQL and print it
        wordCountsDataFrame = spark.sql("select word, count(*) as total from words group by word")
        wordCountsDataFrame.show()
    except:
        pass

#kafka_stream.foreachRDD(process)

ssc.start()
ssc.awaitTermination()


#df = 
#df.schema()
#lines.select("AvSigVersion", "IsBeta") \
#	.write \
#	.save("namesAndAges.json",format="json")
#kafka_stream.select('AvSigVersion','IsBeta','CityIdentifier').show()
