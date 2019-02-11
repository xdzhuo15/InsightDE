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
from schema import StreamSchema
from batch_train import CleanData
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
import mysql.connector
from pyspark.ml.feature import Imputer, VectorAssembler, MinMaxScaler, StringIndexer
import datetime

def predict_risk(rdd, lfModel, pipelineModel):
    ss = SparkSession(rdd.context)
    if rdd.isEmpty():
        return
    df = ss.createDataFrame(rdd, schema = StreamSchema)

    exclude_key_list = ["MachineIdentifier", "CSVId"]

    features = CleanData(df, exclude_key_list)

    transformed_features = pipelineModel.transform(features)
    prediction = lrModel.transform(transformed_features)

    # add original data!
    productID = df.select("MachineIdentifier")
    data.withColumn(["MachineIdentifier", "HasDetections"], [productID, prediction])

    timestamp = encode_timestamp()
    toMysql(output_features, timestamp, False)

def main():
    conf = SparkConf().setAppName("prediction").setMaster(
            "spark://ec2-52-10-44-193.us-west-2.compute.amazonaws.com:7077"
            )
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 1)

    #load saved pipeline, model, and parameters
    lrModel = mlMOdel()
    savedModel = LogisticRegressionModel.load(sc, model.data_file())
    pipe = PiplModel()
    pipelineModel = Pipeline.read.load(pipe.data_file())

    kafka_stream = KafkaUtils.createDirectStream(ssc, ["DeviceRecord"],
            {"metadata.broker.list":"ip-10-0-0-7:9092,ip-10-0-0-11:9092,ip-10-0-0-10:9092"})
    kafka_stream.map(lambda (key, value): json.loads(value))
    kafka_stream = get_kafkastream()
    kafka_stream.foreachRDD(lambda x: predict_risk(x))
    ssc.start()
    ssc.awaitTermination()

if __init__ == "__main__":
    main()
