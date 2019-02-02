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

def handler(message):
    records = message.collect()
    len(records)
    

def convert_json2df(rdd,Schema):
    ss = SparkSession(rdd.context)
    if rdd.isEmpty():
        return
    df = ss.createDataFrame(rdd, schema=Schema)
    df.show()    
   

conf = SparkConf().setAppName("prediction").setMaster(
        "spark://ec2-52-10-44-193.us-west-2.compute.amazonaws.com:7077"
        )

sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

ssc = StreamingContext(sc, 5)
#set up check point
#ssc.checkpoint(checkpointDirectory)

kafka_stream = KafkaUtils.createDirectStream(ssc, 
    ["DeviceRecord"], {"metadata.broker.list":"ip-10-0-0-7:9092,ip-10-0-0-11:9092,ip-10-0-0-10:9092"})
#lines = kafka_stream.map(lambda x: x[1])

#kafka_stream = kafka_stream.map(lambda x: x.decode("utf-8"))

kafka_stream = kafka_stream.map(lambda (key, value): json.loads(value))

cols=["MachineIdentifier","EngineVersion","AvSigVersion","IsBeta","CityIdentifier"]

kafka_stream.foreachRDD(lambda x: convert_json2df(x, cols))

#kafka_stream.foreachRDD(handler)
print 'Event recieved in window!!!!!!: ', kafka_stream.pprint()
#kafka_stream.select('AvSigVersion','IsBeta','CityIdentifier').show()
#df = sc.createDataFrame(kafka_stream)
#kafka_stream = ssc.union(kafka_stream)

#kafka_stream.pprint()
#parsed = kafka_stream.map(lambda x: json.loads(x[1]))


#kafka_stream.foreachRDD(process)

ssc.start()
ssc.awaitTermination()

#kafka_stream.select('AvSigVersion','IsBeta','CityIdentifier').show()
