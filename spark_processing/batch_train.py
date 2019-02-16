#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 26 19:56:05 2019

@author: xdzhuo
"""

from schema import Schema
import json
from pyspark.sql.functions import col
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession, Row, Column
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import boto3
from time_track import *
from io_modules import *
import mysql.connector
import datetime
from freqencoder import FreqEncoder, FreqEncoderModel


class CleanData:
    def __init__(self, data, exclude_key_list):
        self.features = data
        self.exclude = exclude_key_list
        # True or False

    def fill_null(self):
        df = self.features
        cols = df.columns
        for col in cols:
            df = df.fillna({col:0.5})
        return df

    def exclude_cols(self):
        df = self.fill_nul()
        e_list = self.exclude
        return df.select([col for col in df.columns if col not in e_list])

    def count_cols(self):
        train_data = self.exclude_cols()
        categorical_cols = []
        numerical_cols = []
        for types in train_data.dtypes:
            if types[1] == "StringType":
                categorical_cols.append(types[0])
            else:
                numerical_cols.append(types[0])
        return categorical_cols, numerical_cols

    # With customized encoder
    def build_pipeline(self):
        categorical_cols, numerical_cols = self.count_cols()
        stages = []
        for col in categorical_cols:
            encoder = FreqEncoder(inputCol = col, outputCol = col+"_cleaned")
            stages += [encoder]
        finalized_cols = [ c + "_cleaned" for c in categorical_cols ] + [ c for c in numerical_cols]
        assembler = VectorAssembler(inputCols=finalized_cols, outputCol="features_vec")
        stages += [assembler]
        lr = LogisticRegression(featuresCol = "features_vec", labelCol="HasDetections",
                        maxIter=10 )
        stages += [lr]
        pipeline = Pipeline(stages = stages)
        return pipeline


def main():

    timestart = datetime.datetime.now()

    file_name = "train_5000.csv"
    s3 = boto3.resource("s3")
    bucket = s3.Bucket("microsoftpred")
    test_obj = s3.Object(bucket, file_name)

    conf = SparkConf().setAppName("training").setMaster(
            "spark://ec2-52-10-44-193.us-west-2.compute.amazonaws.com:7077"
            )
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.appName("training").getOrCreate()

    df = spark.read.csv("s3a://microsoftpred/{}".format(test_obj.key), header=True, schema=Schema)

    #select top features
    initial_cols = ["HasDetections","SmartScreen","AVProductStatesIdentifier",
                   "CountryIdentifier", "AVProductsInstalled",
                   "Census_OSVersion", "EngineVersion",
                   "AppVersion", "Census_OSBuildRevision",
                   "GeoNameIdentifier", "OsBuildLab"]

    #exclude_key_list = ["MachineIdentifier", "CSVId", "HasDetections]
    exclude_key_list = ["HasDetections"]
    data = df.select(initial_cols)
    train, test = data.randomSplit([0.7, 0.3], seed = 1000)

    # Training
    cleandata = CleanData(train, exclude_key_list)
    train = cleandata.fill_null()
    clean_pipeline = cleandata.build_pipeline()
    pipelineModel = clean_pipeline.fit(train)
    output = PiplModel()
    pipelineModel.save(output.output_name())

    timestamp = time_func.encode_timestamp()
    toMysql(train, timestamp, True)

    timedelta, timeend = run_time(timestart)
    print "Time taken to build pipeline and train: " + str(timedelta) + " seconds"

    # Validation
    cleandata_test = CleanData(test, exclude_key_list)
    test = cleandata_test.fill_null()
    # Need to convert string to doubles, otherwise Pyspark UDF will show errors
    test_new = test.select(*(col(c).cast("float").alias(c) for c in test.columns))
    prediction = pipelineModel.transform(test_new)

    timedelta, timeend = time_func.run_time(timeend)
    print "Time taken to validate the model: " + str(timedelta) + " seconds"

    timestamp = time_func.encode_timestamp()
    toMysql(test, timestamp, True)

if __name__ == "__main__":
    main()
