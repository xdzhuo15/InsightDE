#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 24 23:28:51 2019

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

# os.environ["PYSPARK_PYTHON"]="/usr/bin/python3"
# os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/bin/python3"

# load the latest model
def load_oldest_model(path):
    time_now = datetime.datetime.now()
    # covert file name back to time, sorting
    try:
        for item[1].startswith("string"):
            sg = filename_convert()
    return modelname

def join_data( data1, data2):
    
# save to mysql
def save_pred(data, path, target_table):
    target_table,append(data)
    
    
conf = SparkConf().setAppName("prediction").setMaster(1)
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 2)

kafka_stream = KafkaUtils.createStream(ssc, 
     ["DeviceRecord"], {"metadata.broker.list": brokers})

#convert kafka into dataframe

# load saved parameters from mysql

data_test = df.iloc[:,2:]
features_test = data_clean.feature_engineering(data_test)
label_est = lgbm.predict(features_test)

# load model
SQL_CONNECTION="jdbc:mysql://localhost:3306/bigdata?user=root&password=pwd"
lbgm = lgb.load(modelname)

# Top 4 important features, read data from exported file
feature_imp = data_process.top_features(lgbm, 4 )
final_data = join_data( df, label_est )
save_pred( final_data, SQL_CONNECTION, "Prediction_table")

# connect to flask for UI 4 charts

# combine data with prediction and save to mysql