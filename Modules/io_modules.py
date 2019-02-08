#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Feb  5 12:26:18 2019

@author: xdzhuo
"""
import os
import glob
import json
from time_track import time_functions
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
        time_func = time_functions()
        timestamp = time_func.encode_timestamp()    
        file_name = self.file_path+timestamp+self.file_suf
        return file_name
        
class mlMOdel(IoObject):
    def __init__(self):
        self.folder_path = "hdfs:///model/"
        self.file_path = "hdfs:///model/lrm_model_"
        self.file_suf = ".model"
        
class CountOutput(IoObject):
    def __init__(self):
        self.folder_path = "hdfs:///para/"
        self.file_path = "hdfs:///para/feature_para_"
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
        self.folder_path = "hdfs:///pipeline/"
        self.file_path = "hdfs:///pipeline/pipeline_model_"
        self.file_suf = ""
        
def toMysql():
    def __init__(self,data_frame, file_name):
        self.data = data_frame
        self.table = file_name
        self.sql_url = "jdbc:mysql://localhost/database_name"
        self.sql_driver = "com.mysql.jdbc.Driver"
        self.usr = "your_user_name"
        self.pss = "your_password"
        
    def save_tosql(self):
        self.data.write.format("jdbc").options(
          url=self.sql_url,
          driver=self.sql_driver,
          dbtable=self.table,
          user=self.usr,
          password=self.pss).mode("append").save()
    
    def load_frsql(self): 
        # not need for spark, only query to flask
        source_df = sqlContext.read.format("jdbc").options(
        url=self.sql_url,
        driver=self.sql_driver,
        dbtable=self.table,
        user=self.usr,
        password=self.pss).load()