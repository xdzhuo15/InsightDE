#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 30 12:10:56 2019

@author: xdzhuo
"""

import mysql.connector

connect_args = {
        "host": "ip-10-0-0-8",
        "port": 3306,
        "user": "root",
        "password": "password",
    };
            
db = mysql.connector.connect(**connect_args)            