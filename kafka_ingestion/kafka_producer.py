# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

from kafka import KafkaProducer
import time
import boto3
import pandas as pd
import random
from stream_schema import Schema
from pyspark.sql.types import *

# no need to combine header for each message

file_name = 'test_2000.csv'

s3 = boto3.resource('s3')
bucket = s3.Bucket('microsoftpred')
test_obj = s3.Object(bucket, file_name)

#create Kafka producer that communicates with master node of ec2 instance running Kafka
producer = KafkaProducer(bootstrap_servers = ['localhost:9092'])

data = pd.read_csv(test_obj, index_col=0, schema = Schema)

#simulator that generate N numbers of messages at 1-M random volumes
def user_data(data, N):
    n_data = len(data)
    for i in range(N):
        #M = random.randint(0,n_data)
        M = 100
        index = random.sample(range(0,n_data), M)
        rows = data.loc[index]
        producer.send('DeviceRecord', value=rows)
        producer.flush()
        time.sleep(1)
    
# simulate multiple users  
if __name__ == "__main__":  
    N = 10
    user_data(data, N)
    
    

