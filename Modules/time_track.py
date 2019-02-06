#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Feb  5 12:18:26 2019

@author: xdzhuo
"""
import datetime

class time_functions:
    def __init__(self):
        self.now = datetime.datetime.now()
        self.prev = times_tart
        
    def encode_timestamp(self):
        return unicode(self.now).replace(" ","").replace(":","")

    def run_time(self,time_start):
        timeend = self.now
        return round((timeend-timestart).total_seconds(), 2), timeend 



