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

conf = SparkConf().setAppName('training').setMaster("spark://ec2-52-10-44-193.us-west-2.compute.amazonaws.com:7077")
# sc = SparkContext(conf=conf)
spark = SparkSession.builder.appName('training').getOrCreate()

df = spark.read.csv(file_name, header=True, schema=Schema)
df.printSchema()
df.select('AvSigVersion','IsBeta','CityIdentifier').show()

