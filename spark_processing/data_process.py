#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 26 22:41:27 2019

@author: xdzhuo
"""

# parameters and functions used in training and prediction
import pandas as pd
import numpy as pd
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql as sparksql
from pyspark.sql.functions import col, countDistinct
import datetime

# count the freqency and export 

def encode_timestamp():
    return unicode(datetime.datetime.now()).replace(' ','').replace(':','_');
    
def count_category(col_i, para_table):
    count = df.agg(countDistinct(col(col_i)).alias(col_i))
    para_table.append(count)
    return count

           
def frequency_encoding(col, count):
    # col has mulitple lines
    # catch the case when there is no existing category
    # count is a hash table
    t = train[variable].value_counts().reset_index()
    t = t.reset_index()
    t.loc[t[variable] == 1, 'level_0'] = np.nan
    t.set_index('index', inplace=True)
    max_label = t['level_0'].max() + 1
    t.fillna(max_label, inplace=True)
    return t.to_dict()['level_0']

# loop through all features to process
def feature_engineering(data, train = True):
    features = []
    for item in data_dtypes:
        if item[1].startswith('string'):
            if train == True:
                count = count_category(data[item[0]], para_table)                
            else:
                count = export parameter
            frequency_output = frequency_encoding(data[item[0]],count)    
            features = features.concat([features, frequency_output], axis=1)
        else:
            features = features.concat([features, data[item[0]]], axis=1)
    return features
    
def top_features(model, n):
    feature_list = pd.DataFrame(sorted(zip(
            model.feature_importance(),columns_to_use
            ), reverse=True), columns=['Value','Feature'])
    return feature_list[:n]
    
        

