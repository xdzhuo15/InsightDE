#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 30 12:04:46 2019

@author: xdzhuo
"""
import pandas as pd
from test_schema import Schema
import os
import boto3
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession, Row, Column
from pyspark.sql.types import *


file_name = 'data.csv'

s3 = boto3.resource('s3')
bucket = s3.Bucket('microsoftpred')
test_obj = s3.Object(bucket, file_name)

conf = SparkConf().setAppName('training').setMaster(1)
sc = SparkContext(conf=conf)

df = conf.read.csv(file_name, header=True, schema=Schema)
for i in range(10):
    print df.loc[i]
