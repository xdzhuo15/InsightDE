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


conf = SparkConf().setAppName("prediction").setMaster(
        "spark://ec2-52-10-44-193.us-west-2.compute.amazonaws.com:7077"
        )
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 2)

kafka_stream = KafkaUtils.createStream(ssc, 
     ["DeviceRecord"], {"metadata.broker.list": "localhost:9092"})

kafka_stream.printSchema()
kafka_stream.select('AvSigVersion','IsBeta','CityIdentifier').show()