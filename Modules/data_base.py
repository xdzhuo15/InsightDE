#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Feb  5 12:11:23 2019

@author: xdzhuo
"""
import boto3

def read_s3(filename, bucketname):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucketname)
    test_obj = s3.Object(bucket, file_name)
    return test_obj.key