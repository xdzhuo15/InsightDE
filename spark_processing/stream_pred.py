#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 24 23:28:51 2019

@author: xdzhuo
"""

import read_schema.data_types
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

#c convert kafka output into csv schema


# select categorical features
columnList = [item[0] for item in df.dtypes if item[1].startswith('string')]


# separate 'MachineIdentifier' from training data
[test, label] = feature_engineering.de_label(data)

# feature engineering with schema

# prediction: load model (how to always get the right version?)


# Top 4 important features, read data from exported file

# connect to flask for UI 4 charts

# combine data with prediction and save to mysql