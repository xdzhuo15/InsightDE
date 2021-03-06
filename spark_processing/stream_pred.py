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
from batch_train import CleanData
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
import mysql.connector
from pyspark.ml.feature import VectorAssembler, MinMaxScaler, StringIndexer
import datetime

def predict_risk(rdd, lrModel, pipelineModel, timestamp):
    ss = SparkSession(rdd.context)
    if rdd.isEmpty():
        return
    df = ss.createDataFrame(rdd)

    # top features
    initial_cols = ["SmartScreen","AVProductStatesIdentifier",
                    "CountryIdentifier", "AVProductsInstalled",
                    "Census_OSVersion", "EngineVersion",
                    "AppVersion", "Census_OSBuildRevision",
                    "GeoNameIdentifier", "OsBuildLab"]

    exclude_key_list = []

    features = CleanData(df.select(initial_cols), exclude_key_list)
    data = features.exclude_cols()

    data_new = data.select(*(col(c).cast("float").alias(c) for c in data.columns))

    transformed_features = pipelineModel.transform(data_new)
    HasDetections = lrModel.transform(transformed_features)

    toMysql(HasDetections, timestamp, False)


conf = SparkConf().setAppName("prediction").setMaster(
            "MYSPARK_ADRESS:7077"
            )
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, 1)

#load saved pipeline, model, and parameters
lrModel = mlMOdel()
savedModel = LogisticRegressionModel.load(sc, lrModel.data_file())
pipe = PiplModel()
pipelineModel = Pipeline.read.load(pipe.data_file())

kafka_stream = KafkaUtils.createDirectStream(ssc, ["DeviceRecord"],
            {"metadata.broker.list":"LIST_PRIVATE_IP_OF_BROKERS"})
kafka_stream.map(lambda (key, value): json.loads(value))
kafka_stream = get_kafkastream()
timestampe = encode_timestamp()
kafka_stream.foreachRDD(lambda x: predict_risk(x, lrModel, pipelineModel, timestamp))
ssc.start()
ssc.awaitTermination()
