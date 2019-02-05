# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

from kafka import KafkaProducer
import time
import pandas as pd
import random
from spark_processing.batch_train import read_s3

File_name = "test_2000.csv"
Bucket_name = "microsoftpred"

#simulator that generates N numbers of messages at 1-M random volumes
def user_data(data, N):
    n_data = len(data)
    for i in range(N):
        #M = random.randint(0,n_data)
        M = 100
        for j in range(M):
            index = random.randint(0, n_data-1)
            row = data.loc[index]
            producer.send('DeviceRecord', row.to_json())
        producer.flush()
        time.sleep(1)
    
if __name__ == "__main__":     
    producer = KafkaProducer(bootstrap_servers = "localhost:9092")    
    test_data = read_s3(File_name, Bucket_name)      
    data = pd.read_csv(test_data, index_col=0 )   
    N = 10
    user_data(data, N)
    
    

