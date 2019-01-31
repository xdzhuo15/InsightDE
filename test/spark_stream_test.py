#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 30 12:07:24 2019

@author: xdzhuo
"""

from kafka_ingestion import stream_schema
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import lightbgm as lgb
import os
from pyspark import SparkConf
from pyspark.sql import SQLContext, SparkSession, Row, Column
from pyspark.sql.types import *
import datetime
import data_process

conf = SparkConf().setAppName("prediction").setMaster(1)
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 2)

kafka_stream = KafkaUtils.createStream(ssc, 
     ["DeviceRecord"], {"metadata.broker.list": brokers})

print kafka_stream