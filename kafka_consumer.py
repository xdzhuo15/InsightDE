#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 23 12:55:30 2019

@author: xdzhuo
"""

from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'rawDBGData',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group')

for message in consumer:
    print("{0}:{1}:{2}: key={3} value={4}".format(message.topic, message.partition,
                                             message.offset, message.key,
                                             message.value))