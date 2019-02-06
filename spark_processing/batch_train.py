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
from pyspark.ml.feature import Imputer, VectorAssembler, MinMaxScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline    
from pyspark.ml.evaluation import BinaryClassificationEvaluator 
from data_base import read_s3
from time_track import *
from io_modules import *

def exclude_cols(df, exclude_key_list):
    return df.select([col for col in df.columns if col not in exclude_key_list])
    
# Replace Null with Empty 
def clean_category_train(features):
    categorical_cols = []
    numerical_cols = []
    for col in features.columns:
        data_type = col.dtypes
        if data_type[1] == "StringType":
            one_col = features.select(col).na.fill("Empty")
            features.withColumn(col+"_encoded",one_col)
            categorical_cols.append(col)
        else:
            numerical_cols.append(col)
    return categorical_cols, numerical_cols
        
# Count disctinct for strings and save to HDFS json
# Map count for categorical variables (training only )
def map_category(features, categorical_cols, numerical_cols):  
    para_dict = {}
    for col in numerical_cols:
        para_dict[col] = {}
    for col in categorical_cols:
        select_col = col + "_encoded"
        one_col = features.select(select_col)
        category_dict = one_col.grouBy().count() 
        para_dict[select_col] = category_dict
        mapped_col=one_col.na.replace(category_dict, 1)
        features.withColumn(col+"_mapped",mapped_col)
    timestamp = encode_timestamp()
    data_path = "hdfs:///para/feature_para_"+ timestamp+".json" 
    with open(data_path, 'w') as f:                   
        json.dump(para_dict, f)        

def build_pipeline(features, categorical_cols, numerical_cols):
    stages = []
    for n_col in numerical_cols:
        impute = Imputer(inputCol = n_col, outputCol = n_col + "_cleaned")
    stages += [impute]
    selected_cols = [ c + "_mapped" for c in categorical_cols ]+[ c + "_cleaned" for c in numerical_cols ]
    for col in selected_cols:
        norm_feature = MinMaxScaler(inputCol = col, outputCol=col + "_norm")
    stages +=[norm_feature]
    finalized_cols = [ c + "_norm" for c in selected_cols ]
    assembler = VectorAssembler(inputCols=selected_features, outputCol="features_vec")
    stages +=[assembler]
    return Pipeline(stages = stages)

def main():
    timestart = datetime.datetime.now()   
    
    # import training data from s3
    file_name = "train_5000.csv"
    bucket_name = "microsoftpred"

    train_data=read_s3(bucket_name, file_name)
    conf = SparkConf().setAppName("training").setMaster(
            "spark://ec2-52-10-44-193.us-west-2.compute.amazonaws.com:7077"
            )
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.appName("training").getOrCreate()
    df = spark.read.csv(train_data, header=True, schema=Schema) 
    label = df.select("HasDetections")
    features = exclude_cols(df,["MachineIdentifier", "CSVId"])
    categorical_cols, numerical_cols = clean_category_train(features)
    map_category(features, categorical_cols, numerical_cols)
    
    features.cache()
    features.count()
    timedelta, timeend = run_time(timestart) 
    print "Time taken to clean data: " + str(timedelta) + " seconds"
    
    pipeline = build_pipeline(features, categorical_cols, numerical_cols)
    final_cols = numerical_cols + [ c + "_mapped" for c in categorical_cols ]
    features_final = features.select(final_cols)
    train, test = features_final.randomSplit([0.7, 0.3], seed = 1000) 
    print("Training Dataset Count: " + str(train.count()))
    print("Test Dataset Count: " + str(test.count()))
    pipelineModel = pipeline.fit(features)
    features = pipelineModel.transform(features)
    
    features.cache()
    features.count()
    timedelta, timeend = run_time(timeend) 
    print "Time taken to built pipeline: " + str(timedelta) + " seconds"
    
    lr = LogisticRegression(featuresCol = "features_vec", labelCol = "HasDetections", 
                            maxIter=10, regParam=0.3, elasticNetParam=0.8 )
    lrModel = lr.fit(train)
    trainingSummary = lrModel.summary
    
    validation = lrModel.transform(test)
    evaluator = BinaryClassificationEvaluator()
    print('Test Area Under ROC', evaluator.evaluate(validation))
    
    timedelta, timeend = run_time(timeend) 
    print "Time taken to train the model: " + str(timedelta) + " seconds"
    
    timestamp = encode_timestamp()
    data_path = "hdfs:///model/lrm_model_"+ timestamp+".model"
    lrModel.save(sc, data_path)
    print("Coefficients: " + str(lrModel.coefficients))
    # save df in sql
    # connect to flask for 4 charts of top features
    
if __name__ == "__main__": 
    main()
 

#SQL_CONNECTION="jdbc:mysql://localhost:3306/bigdata?user=root&password=pwd"

# Top 4 important features, export their label for prediction


# connect to flask for UI 4 charts

# connext to flask for ML model 


