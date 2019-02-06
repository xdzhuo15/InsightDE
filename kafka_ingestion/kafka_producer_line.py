# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
from smart_open import smart_open
from kafka import KafkaProducer
import time

if __name__ == "__main__":  
    file_name = "test_2000.csv"
    producer = KafkaProducer(bootstrap_servers = "localhost:9092")    
    for line in smart_open("s3://microsoftpred/test_2000.csv"):
        producer.send('DeviceRecord',line)
        producer.flush()
        time.sleep(3)

    

