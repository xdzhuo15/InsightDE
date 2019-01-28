#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 26 19:56:05 2019

@author: xdzhuo
"""

import pandas as pd
import numpy as pd
import read_schema.data_types
from sklearn.model_selection import train_test_split
import lightbgm as lgb
from sklearn import model_selection, preprocessing, metrics
import os



file_name = 'data/train.csv'

df = spark.read.csv(file_name, header=True, schema=data_types)


# select categorical features
columnList = [item[0] for item in df.dtypes if item[1].startswith('string')]


# separate 'MachineIdentifier' from training data
[train, label] = feature_engineering.de_label(data)

# separate "hasdetection' from training data

# feature engineering with schema

# training and validation

new_train, new_val, target_train, target_val = train_test_split(new_train, 
                    target, test_size=0.2, random_state=42)
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


new_train = lgb.Dataset(new_train.values, label=target_train)
new_val = lgb.Dataset(new_val.values, label=target_val)
num_round = 1000
clf = lgb.train(param, new_train, num_round, valid_sets = [new_train, new_val], 
                verbose_eval=10, early_stopping_rounds = 25)


# Top 4 important features, export their label for prediction
feature_imp = pd.DataFrame(sorted(zip(clf.feature_importance(),columns_to_use),
                                  reverse=True), columns=['Value','Feature'])
feature_imp.head()
# connect to flask for UI 4 charts

# connext to flask for ML model 

# save ML model (add time stamps)

# save count for categorical, top features