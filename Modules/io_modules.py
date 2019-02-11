#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Feb  5 12:26:18 2019

@author: xdzhuo
"""
import os
import glob
import json
from time_track import *
#from pyspark.sql import SQLContext

def get_latestfile(folder_path):
    list_of_files = glob.glob(folder_path+"/*") # * means all if need specific format then *.csv
    latest_file = max(list_of_files, key=os.path.getctime)
    return latest_file

class IoObject:
# ML: machine learning model, PR: mapped feature transformation
    def __init__(self):
        self.folder_path = ""
        self.file_path = ""
        self.file_suf = ""
        self.data_file = self.folder_path+get_latestfile(self.folder_path)

    def output_name(self):
        timestamp = encode_timestamp()
        file_name = self.file_path+timestamp+self.file_suf
        return file_name

class mlMOdel(IoObject):
    def __init__(self):
        self.folder_path = "model/"
        self.file_path = "model/lrm_model_"
        self.file_suf = ".model"

class CountOutput(IoObject):
    def __init__(self):
        self.folder_path = "para/"
        self.file_path = "para/feature_para_"
        self.file_suf = ".json"

    def read_file(self):
        try:
            with open(self.data_file) as f:
                para_json = json.load(f)
                return para_json
        except:
            print "No parameters or modles in place!"
            return {}

class PiplModel(IoObject):
    def __init__(self):
        self.folder_path = "/pipeline/"
        self.file_path = "/pipeline/pipeline_model_"
        self.file_suf = ""


def toMysql(df, timestamp, isTrain ="True"):
    timestamp = encode_timestamp()
    if isTrain == True:
        db_name = "Training"
        mood = "write"
    else:
        db_name = "Prediction"
        save_mode = "append"
    df.write.format('jdbc').options(
            url="jdbc:mysql://ec2-34-211-3-37.us-west-2.compute.amazonaws.com:3306/{}".format(db_name),
            driver='com.mysql.cj.jdbc.Driver',
            dbtable="Data_"+timestamp,
            user="USERNAME",
            password="PASSWORD").mode(save_mode).save()
