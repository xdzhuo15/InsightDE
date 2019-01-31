#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 27 19:24:49 2019

@author: xdzhuo
"""

from pyspark.sql.types import *
# Defined on the true meaning of the data one by one

Schema = StructType([
    StructField("MachineIdentifier", StringType()),     
    StructField("EngineVersion", StringType()),
    StructField("AvSigVersion", StringType()),
    StructField("IsBeta", ShortType()),
    StructField("CityIdentifier", IntegerType())
    ])