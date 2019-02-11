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
from pyspark.ml.feature import Imputer, VectorAssembler, MinMaxScaler, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import boto3
from time_track import *
from io_modules import *
import mysql.connector


class CleanData:
    def __init__(self, data, exclude_key_list, is_train):
        self.features = data
        self.exclude_cols = exclude_key_list
        # True or False
        self.isTrain = is_train

    def exclude_cols(self):
        df = self.features
        e_list = self.exclude_cols
        self.remove_label = df.select([col for col in df.columns if col not in e_list])
        return self.remove_label

    def count_cols(self):
        train_data=self.remove_label
        self.categorical_cols = []
        self.numerical_cols = []
        for types in train_data.dtypes:
            if types[1] == "StringType":
               self.categorical_cols.append(col)
            else:
                self.numerical_cols.append(col)
        return self.categorical_cols, self.numerical_cols

    def fill_nullstring(self):
        self.filter_category = self.remove_label
        for col in self.categorical_cols:
            one_col = self.remove_label.select(col).na.fill("Empty")
            self.filter_category.withColumn(col+"_encoded",one_col)
            return self.filter_category

    # Count disctinct for strings and save to HDFS json
    # Map count for categorical variables
    def map_category_train(self):
        para_dict = {}
        self.mapped_t = self.filter_category
        for col in self.categorical_cols:
            select_col = col + "_encoded"
            one_col = self.filter_category.select(select_col)
            category_dict = one_col.grouBy().count()
            para_dict[col] = category_dict
            mapped_col=one_col.na.replace(category_dict, 1)
            self.mapped_t.withColumn(col+"_mapped",mapped_col)
        output = CountOutput()
        with open(output.output_name(), 'w') as f:
            json.dump(para_dict, f)
        return self.mapped_trn

    # Replace NULL category with Empty (steaming only )
    # Map to count values
    def map_category_pred(self):
        output = CountOutput()
        para_json = output.read_file()
        self.mapped_s=self.filter_category
        if para_json != {}:
            for key in para_json.keys():
                select_col = key + "_encoded"
                one_col = self.mapped.select(select_col)
                mapped_col=one_col.na.replace(para_json[key], 1)
                self.mapped_s.withColumn(key+"_mapped",mapped_col)
        return self.mapped_str

    # Without customized module
    def build_pipeline(self):
        if self.isTrain == True:
            self.final = self.mapped_trn
        else:
            self.final = self.mapped_str
        stages = []
        for col in self.numerical_cols:
            imputer = Imputer(inputCol = col, outputCol = col + "_cleaned")
            stages += [imputer]
        selected_cols = [ c + "_mapped" for c in self.categorical_cols ]+[ c + "_cleaned" for c in self.numerical_cols ]
        for col in selected_cols:
            norm_feature = MinMaxScaler(inputCol = col, outputCol=col + "_norm")
            stages +=[norm_feature]
        self.finalized_cols = [ c + "_norm" for c in selected_cols ]
        self.selected_features = self.final.select(self.finalized_cols_1)
        assembler = VectorAssembler(inputCols=self.selected_features_1, outputCol="features_vec")
        stages +=[assembler]
        self.pipeline = Pipeline(stages = stages)
        return self.pipeline

    # With feature library to simplify
    def build_pipeline_sp(self):
        train_data = self.remove_label
        stages = []
        input_numerical =  [ col for col in self.numerical_cols]
        output_numerical = [ col + "_cleaned" for col in self.numerical_cols ]
        imputer = Imputer(inputCol = input_numerical, outputCol = output_numerical)
        stages += [imputer]
        input_numerical =  [ col for col in categorical_cols ]
        output_numerical = [ col + "_cleaned" for col in self.categorical_cols ]
        encoder = StringIndexer(inputCol = col, outputCol = col + "_cleaned")
        stages += [encoder]
        selected_cols = [ c + "_cleaned" for c in self.categorical_cols ]+[ c + "_cleaned" for c in self.numerical_cols ]
        for col in selected_cols:
            norm_feature = MinMaxScaler(inputCol = col, outputCol=col + "_norm")
            stages += [norm_feature]
        self.finalized_cols_sp = [ c + "_norm" for c in selected_cols ]
        self.selected_features_sp = self.final.select(self.finalized_cols_sp)
        assembler = VectorAssembler(inputCols=self.selected_features, outputCol="features_vec")
        stages += [assembler]
        self.pipeline_sp = Pipeline(stages = stages)
        return self.pipeline_sp


def main():
    #time_func = time_functions()
    #timestart = time_func.now()

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
    features = CleanData(df, exclude_key_list, True)

    clean_pipeline = features.pipeline_sp
    pipelineModel = clean_pipeline.fit(clean_features)
    transformed_features = pipelineModel.transform(clean_features)

    output = PiplModel()
    pipelineModel.write.save(output.output_name())

    #time_func = time_functions()
    #timedelta, timeend = time_func.run_time(timeend)
    #print "Time taken to build pipeline: " + str(timedelta) + " seconds"

    selected_cols = [ "features_vec"] + features.finalized_cols()
    training_data =  transformed_features.select(selected_cols)
    training_data.withColumn(labels)
    train, test = training_data.randomSplit([0.7, 0.3], seed = 1000)
    print("Training Dataset Count: " + str(train.count()))
    print("Test Dataset Count: " + str(test.count()))

    lr = LogisticRegression(featuresCol = "features_vec", labelCol = "HasDetections",
                            maxIter=10, regParam=0.3, elasticNetParam=0.8 )
    lrModel = lr.fit(train)
    #time_func = time_functions()
    #timedelta, timeend = time_func.run_time(timeend)
    #print "Time taken to train the model: " + str(timedelta) + " seconds"

    #trainingSummary = lrModel.summary
    validation = lrModel.transform(test)
    evaluator = BinaryClassificationEvaluator()
    print('Test Area Under ROC', evaluator.evaluate(validation))

    output = mlMOdel()
    lrModel.save(sc, output.output_name())
    print("Coefficients: " + str(lrModel.coefficients))

    # save df in sql (add original data)
    productID = training_data.select("MachineIdentifier")
    output_features.withColumn("MachineIdentifier", productID)

    #time_func = time_functions()
    #timestamp = time_func.encode_timestamp()
    timestamp = "102034"
    toMysql(output_features, True, timestamp)

if __name__ == "__main__":
    main()


#SQL_CONNECTION="jdbc:mysql://localhost:3306/bigdata?user=root&password=pwd"

# Top 4 important features, export their label for prediction


# connect to flask for UI 4 charts

# connext to flask for ML model
