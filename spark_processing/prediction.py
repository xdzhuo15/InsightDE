#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 24 23:28:51 2019

@author: xdzhuo
"""

from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import os
from pyspark import SparkConf

os.environ["PYSPARK_PYTHON"]="/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/bin/python3"

conf = SparkConf().setAppName("process").setMaster("local[*]")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 2)

kafkaStream = KafkaUtils.createStream(ssc, 
     ['DeviceRecord'], [consumer group id], {"metadata.broker.list": brokers})

