#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 26 19:56:05 2019

@author: xdzhuo
"""

import pandas as pd
import numpy as pd
from train_schema import Schema
from sklearn.model_selection import train_test_split
import lightbgm as lgb
from sklearn import model_selection, preprocessing, metrics
import os
import boto3
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession, Row, Column
from pyspark.sql.types import *
import data_process
import data_time



# import training data from s3
file_name = 'train_5000.csv'

s3 = boto3.resource('s3')
bucket = s3.Bucket('microsoftpred')
test_obj = s3.Object(bucket, file_name)

conf = SparkConf().setAppName('training').setMaster(1)
sc = SparkContext(conf=conf)

df = spark.read.csv(file_name, header=True, schema=Schema)

n_col = len(df.columns)
data = df.iloc[:,2:n_col-1]
label = df.iloc[:, n_col-1]
 

features = data_clean.feature_engineering(data)

# training and validation

data_trn, data_val, label_trn, label_val = train_test_split(data, 
                    label, test_size=0.3, random_state=0)


param = {'num_leaves': 200,
         'min_data_in_leaf': 60, 
         'objective':'binary',
         'max_depth': -1,
         'learning_rate': 0.1,
         "min_child_samples": 20,
         "boosting": "gbdt",
         "feature_fraction": 0.8,
         "bagging_freq": 1,
         "bagging_fraction": 0.8 ,
         "bagging_seed": 17,
         "metric": 'auc',
         "lambda_l1": 0.1,
         "verbosity": -1,
         "n_jobs":-1}

lgb_trn = lgb.Dataset(data_trn, label_trn)
lgb_val = lgb.Dataset(data_val, label_val)

num_round = 30
lgbm = lgb.train(params,
                lgb_trn,
                num_round,
                valid_sets=lgb_val,
                verbose_eval=10,
                early_stopping_rounds=25)

SQL_CONNECTION="jdbc:mysql://localhost:3306/bigdata?user=root&password=pwd"

timestamp = encode_timestamp()
model_name = 'lgbm_'+ timestamp + '.txt'
save_path = '/' # to my sql probably not good
lgbm.save_model(save_path+model_name)
# Top 4 important features, export their label for prediction

feature_imp = data_process.top_features(lgbm, 4 )

schema_site = sqlContext.createDataFrame(feature_imp)
schema_site.registerTempTable('feature_imp'+timestamp)
schema_site.write.jdbc(url=mysql_url, table='table1', mode='append')



# connect to flask for UI 4 charts

# connext to flask for ML model 


