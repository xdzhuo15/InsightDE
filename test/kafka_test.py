# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

from kafka import KafkaProducer
import time
import boto3
import pandas as pd
#import random
from test_schema import Schema

# no need to combine header for each message

file_name = 'data.csv'

s3 = boto3.resource('s3')
bucket = s3.Bucket('microsoftpred')
test_obj = s3.Object(bucket, file_name)

#create Kafka producer that communicates with master node of ec2 instance running Kafka
producer = KafkaProducer(bootstrap_servers = ['localhost:9092'])

data = pd.read_csv(test_obj, index_col=0, schema = Schema)
for i in range(11):
    rows = data.loc(i)
    print rows
    producer.send('DeviceRecord', value=rows)
    producer.flush()
    time.sleep(10)

    
    

