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

# count the freqency and export 
def count_category():

def frequency_encoding():
    t = train[variable].value_counts().reset_index()
    t = t.reset_index()
    t.loc[t[variable] == 1, 'level_0'] = np.nan
    t.set_index('index', inplace=True)
    max_label = t['level_0'].max() + 1
    t.fillna(max_label, inplace=True)
    return t.to_dict()['level_0']

# separate 'MachineIdentifier' from training data
def de_label(data):
    
# Top 4 important features    
    
