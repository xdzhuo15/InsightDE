#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 24 23:28:51 2019

@author: xdzhuo
"""
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark import SparkConf
from pyspark.sql import SQLContext, SparkSession, Row, Column
from pyspark.sql.types import *
from time_track import *
from io_modules import *
import json

# Replace NULL category with Empty (steaming only )
# Map to count values
def clean_category(features):
    categorical_cols = [] 
    numerical_cols = []
    para_json = read_file("PR")
    if para_json != {}: 
        for key in para_json.keys():
            if para_json[key] == {}:
                numerical_cols.append(key)
            else:
                col = key.replace("_encoded","")
                if col in features.columns:
                    one_col = features.select(col).na.fill("Empty")
                    mapped_col=one_col.cast(StringType).na.replace(para_json[key], 1)
                    features.withColumn(col+"_mapped",mapped_col)
                    categorical_cols.append(col)
    return categorical_cols, numerical_cols

def convert_json2df(rdd):
    ss = SparkSession(rdd.context)
    if rdd.isEmpty():
        return
    df = ss.createDataFrame(rdd)

def join_data( data1, data2):
    
# save to mysql
def save_pred(data, path, target_table):
    target_table,append(data)
    

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

def main():
    conf = SparkConf().setAppName("prediction").setMaster(
            "spark://ec2-52-10-44-193.us-west-2.compute.amazonaws.com:7077"
            )
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 5)
    kafka_stream = KafkaUtils.createDirectStream(ssc, ["DeviceRecord"], 
            {"metadata.broker.list":"ip-10-0-0-7:9092,ip-10-0-0-11:9092,ip-10-0-0-10:9092"})    
    kafka_stream.map(lambda (key, value): json.loads(value))
    kafka_stream = get_kafkastream()
    kafka_stream.foreachRDD(lambda x: convert_json2df(x))
    ssc.start()
    ssc.awaitTermination()
    
if __init__ == "__main__":
    main()    