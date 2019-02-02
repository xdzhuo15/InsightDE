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
    

def convert_json2df(rdd):
    ss = SparkSession(rdd.context)
    if rdd.isEmpty():
        return
    #test = rdd.first()
    #print type(test)
    df = ss.createDataFrame(rdd)
    df.show()    
    df.select("IsBeta").show()
    df.printSchema()

conf = SparkConf().setAppName("prediction").setMaster(
        "spark://ec2-52-10-44-193.us-west-2.compute.amazonaws.com:7077"
        )

sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

ssc = StreamingContext(sc, 5)

kafka_stream = KafkaUtils.createDirectStream(ssc, 
    ["DeviceRecord"], {"metadata.broker.list":"ip-10-0-0-7:9092,ip-10-0-0-11:9092,ip-10-0-0-10:9092"})

#kafka_stream = kafka_stream.map(lambda x: x[1])

#kafka_stream = kafka_stream.map(lambda x: x.decode("utf-8"))

kafka_stream = kafka_stream.map(lambda (key, value): json.loads(value))

#cols=["MachineIdentifier","EngineVersion","AvSigVersion","IsBeta","CityIdentifier"]

kafka_stream.foreachRDD(lambda x: convert_json2df(x))

print 'Event recieved in window!!!!!!: '

ssc.start()
ssc.awaitTermination()


