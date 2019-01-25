# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

from kafka import KafkaProducer
import time
import boto3
import pandas as pd


#create Kafka producer that communicates with master node of ec2 instance running Kafka
producer = KafkaProducer(bootstrap_servers = ['localhost:9092'])

#creates bucket that points to data
s3 = boto3.client('s3')
obj = s3.get_object(Bucket='microsoftpred', Key='test.csv')

data = pd.read_csv(obj)

#read through each line of csv and send the line to the kafka topic
for index, row in data.iterrows():
    producer.send('DeviceRecord', value=row)
    time.sleep(1)
 
