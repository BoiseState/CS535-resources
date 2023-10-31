#!/usr/bin/env python3
# coding: utf-8

import findspark
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[*]').appName("case-analysis").getOrCreate()
sc = spark.sparkContext

rdd1 = sc.wholeTextFiles('input/')
rdd2 = rdd1.flatMap(lambda data: data[1].split())
rdd3 = rdd2.flatMap(lambda x: [(y, 1) if y.isupper()==True else (y.upper(), 0) for y in x] )
rdd4 = rdd3.groupByKey().mapValues(lambda x: sum(x)/len(x)*100)

# Collect rdd4 for diagnostics
rdd4.collect()

rdd5 = rdd4.filter(lambda x: x[0].isalpha()) 
rdd5 = rdd5.sortByKey()

# We need to collect it to print in the driver
result = rdd5.collect()
for i in result:
    print(i)



