#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Feb  5 12:26:18 2019

@author: xdzhuo
"""
import os
import glob
import json

def get_latestfile(folder_path):
    list_of_files = glob.glob(folder_path+"/*") # * means all if need specific format then *.csv
    latest_file = max(list_of_files, key=os.path.getctime)
    return latest_file

# ML: machine learning model, PR: mapped feature transformation    
def read_file(file_type):
    if file_type == "ML":
        folder_path = "hdfs:///model/"
    elif file_type == "PR":
        folder_path = "hdfs:///para/"
    data_file = folder_path+get_latestfile(folder_path)
    try:
        with open(data_file) as f:
            para_json = json.load(f)
        return para_json 
    except:
        print "No parameters or modles in place!"  
        return {} 