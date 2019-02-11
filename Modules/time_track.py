#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Feb  5 12:18:26 2019

@author: xdzhuo
"""
import datetime

def encode_timestamp(self):
    return unicode(datetime.datetime.now()).replace(" ","").replace(":","")

def run_time(time_start):
        timeend = datetime.datetime.now()
        return round((timeend-timestart).total_seconds(), 2), timeend
