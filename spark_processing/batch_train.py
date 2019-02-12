#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 26 19:56:05 2019

@author: xdzhuo
"""

from schema import Schema
import json
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession, Row, Column
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, MinMaxScaler, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import boto3
from time_track import *
from io_modules import *
import mysql.connector
import datetime


class CleanData:
    def __init__(self, data, exclude_key_list):
        self.features = data
        self.exclude = exclude_key_list
        # True or False

    def exclude_cols(self):
        df = self.features
        e_list = self.exclude
        remove_label = df.select([col for col in df.columns if col not in e_list])
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

    def fill_nullstring(self):
        filter_category = self.exclude_cols()
        categorical_cols, numerical_cols = self.count_cols()
        for col in categorical_cols:
            one_col = filter_category.select(col).na.fill("Empty")
            filter_category.withColumn(col+"_encoded",one_col)
            return filter_category

    # Count disctinct for strings and save to HDFS json
    # Map count for categorical variables
    def map_category_train(self):
        para_dict = {}
        mapped_trn = self.fill_nullstring()
        categorical_cols, numerical_cols = self.count_cols()
        for col in categorical_cols:
            select_col = col + "_encoded"
            one_col = mapped_trn.select(select_col)
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
        mapped_str=self.fill_nullstring()
        if para_json != {}:
            for key in para_json.keys():
                select_col = key + "_encoded"
                one_col = mapped_str.select(select_col)
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
        selected_features = final.select(finalized_cols)
        assembler = VectorAssembler(inputCols=selected_features, outputCol="features_vec")
        stages +=[assembler]
        pipeline = Pipeline(stages = stages)
        return pipeline, selected_features

    # With feature library to simplify
    def build_pipeline_sp(self):
        train_data = self.exclude_cols()
        categorical_cols, numerical_cols = self.count_cols()
        stages = []
        for col in categorical_cols:
            encoder = StringIndexer(inputCol = col, outputCol = col+"_cleaned")
            stages += [encoder]
        selected_cols = [ c + "_cleaned" for c in categorical_cols ]+[ c for c in numerical_cols ]
        for col in selected_cols:
            norm_feature = MinMaxScaler(inputCol = col, outputCol=col + "_norm")
            stages += [norm_feature]
        finalized_cols_sp = [ c + "_norm" for c in selected_cols ]
        selected_features = train_data.select(finalized_cols_sp)
        assembler = VectorAssembler(inputCols=selected_features, outputCol="features_vec")
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
            "spark://ec2-52-10-44-193.us-west-2.compute.amazonaws.com:7077"
            )
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.appName("training").getOrCreate()

    df = spark.read.csv("s3a://microsoftpred/{}".format(test_obj.key), header=True, schema=Schema)

    exclude_key_list = ["MachineIdentifier", "CSVId", "HasDetections"]
    labels = df.select("HasDetections")
    features = CleanData(df, exclude_key_list)

    clean_pipeline = features.build_pipeline_sp()
    pipelineModel = clean_pipeline.fit(features)
    transformed_features = pipelineModel.transform(features)

    output = PiplModel()
    pipelineModel.write.save(output.output_name())

    timedelta, timeend = run_time(timestart)
    print "Time taken to build pipeline: " + str(timedelta) + " seconds"

    selected_cols = [ "features_vec"] + selected_features
    training_data =  transformed_features.select(selected_cols)
    training_data.withColumn(labels)
    train, test = training_data.randomSplit([0.7, 0.3], seed = 1000)
    print("Training Dataset Count: " + str(train.count()))
    print("Test Dataset Count: " + str(test.count()))

    lr = LogisticRegression(featuresCol = "features_vec", labelCol = "HasDetections",
                            maxIter=10, regParam=0.3, elasticNetParam=0.8 )
    lrModel = lr.fit(train)

    timedelta, timeend = time_func.run_time(timeend)
    print "Time taken to train the model: " + str(timedelta) + " seconds"

    validation = lrModel.transform(test)
    evaluator = BinaryClassificationEvaluator()
    print('Test Area Under ROC', evaluator.evaluate(validation))

    output = mlMOdel()
    lrModel.save(sc, output.output_name())
    print("Coefficients: " + str(lrModel.coefficients))

    # save df in sql (add original data)
    productID = training_data.select("MachineIdentifier")
    output_features.withColumn("MachineIdentifier", productID)

    timestamp = time_func.encode_timestamp()
    toMysql(output_features, timestamp, True)

if __name__ == "__main__":
    main()


#SQL_CONNECTION="jdbc:mysql://localhost:3306/bigdata?user=root&password=pwd"

# Top 4 important features, export their label for prediction


# connect to flask for UI 4 charts

# connext to flask for ML model
