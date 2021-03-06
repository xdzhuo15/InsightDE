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
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
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

    def exclude_cols(self):
        df = self.features
        e_list = self.exclude
        remove_label = df.select([col for col in df.columns if col not in e_list])
        for col in remove_label.columns:
            remove_label = remove_label.fillna({col:0.5})
        return remove_label

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

    # Count disctinct for strings and save to HDFS json
    # Map count for categorical variables
    def map_category_train(self):
        para_dict = {}
        mapped_trn = self.exclude_cols()
        categorical_cols, numerical_cols = self.count_cols()
        for col in categorical_cols:
            one_col = mapped_trn.select(col)
            category_dict = one_col.grouBy().count()
            para_dict[col] = category_dict
            mapped_col=one_col.na.replace(category_dict, 1)
            mapped_trn.withColumn(col+"_mapped",mapped_col)
        output = CountOutput()
        with open(output.output_name(), 'w') as f:
            json.dump(para_dict, f)
        return mapped_trn

    # Replace NULL category with Empty (steaming only )
    # Map to count values
    def map_category_pred(self):
        output = CountOutput()
        para_json = output.read_file()
        mapped_str=self.exclude_cols()
        if para_json != {}:
            for key in para_json.keys():
                one_col = mapped_str.select(key)
                mapped_col=one_col.na.replace(para_json[key], 1)
                mapped_str.withColumn(key+"_mapped",mapped_col)
        return mapped_str

    # Without customized module
    def build_pipeline(self):
        final = self.map_category_pred()
        categorical_cols, numerical_cols = self.count_cols()
        stages = []
        selected_cols = [ c + "_mapped" for c in categorical_cols]+[ c for c in numerical_cols ]
        for col in selected_cols:
            norm_feature = MinMaxScaler(inputCol = col, outputCol=col + "_norm")
            stages +=[norm_feature]
        finalized_cols = [ c + "_norm" for c in selected_cols ]
        assembler = VectorAssembler(inputCols=finalized_cols, outputCol="features_vec")
        stages +=[assembler]
        pipeline = Pipeline(stages = stages)
        return pipeline

    # With feature library to simplify
    def build_pipeline_sp(self):
        train_data = self.exclude_cols()
        categorical_cols, numerical_cols = self.count_cols()
        stages = []
        for col in categorical_cols:
            encoder = FreqEncoder(inputCol = col, outputCol = col+"_cleaned")
            stages += [encoder]
        finalized_cols = [ c + "_cleaned" for c in categorical_cols ] + [ c for c in numerical_cols]
        assembler = VectorAssembler(inputCols=finalized_cols, outputCol="features_vec")
        stages += [assembler]
        pipeline = Pipeline(stages = stages)
        return pipeline


def main():

    timestart = datetime.datetime.now()

    file_name = "train_5000.csv"
    s3 = boto3.resource("s3")
    bucket = s3.Bucket("microsoftpred")
    test_obj = s3.Object(bucket, file_name)

    conf = SparkConf().setAppName("training").setMaster(
            "MYSPARK_ADRESS:7077"
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
    features = CleanData(data, exclude_key_list)

    data = features.exclude_cols()
    clean_pipeline = features.build_pipeline_sp()
    pipelineModel = clean_pipeline.fit(data)

    # Need to convert string to doubles, otherwise Pyspark UDF will show errors
    data_new = df.select(*(col(c).cast("float").alias(c) for c in df.columns))
    final_feature = pipelineModel.transform(data_new)

    output = PiplModel()
    pipelineModel.save(output.output_name())

    timedelta, timeend = run_time(timestart)
    print "Time taken to build pipeline: " + str(timedelta) + " seconds"

    train, test = final_feature.randomSplit([0.7, 0.3], seed = 1000)

    lr = LogisticRegression(featuresCol = "features_vec", labelCol = "HasDetections",
                            maxIter=10 )
    lrModel = lr.fit(train)

    timedelta, timeend = time_func.run_time(timeend)
    print "Time taken to train the model: " + str(timedelta) + " seconds"

    validation = lrModel.transform(test)

    output = mlMOdel()
    lrModel.save(sc, output.output_name())

    timestamp = time_func.encode_timestamp()
    toMysql(df, timestamp, True)

if __name__ == "__main__":
    main()
