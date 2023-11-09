#!/usr/bin/env python3
# coding: utf-8

import os
import itertools
import sys
from pyspark import SparkContext, SparkConf

if (len(sys.argv) != 2):
    print("Usage: case-analysis.py <input folder>")
    sys.exit(1)

conf = SparkConf().setAppName('pagerank')
sc = SparkContext.getOrCreate(conf=conf)

rdd = sc.wholeTextFiles(sys.argv[1])
rdd.collect()

rdd2 = rdd.flatMap(lambda x: [d.split() for d in x[1].split('\n') if d!=''])
rdd3 = rdd2.groupByKey().mapValues(list)

rdd4 = rdd3.map(lambda x: (x[0], 1.0))

max_iter = 12
for i in range(5):
    update_cont = rdd3.join(rdd4).flatMap(lambda x:[(b, x[1][1]/len(x[1][0])) for b in x[1][0]]).groupByKey().mapValues(sum)
    rdd4 = update_cont

result = rdd4.collect()
for x in result:
    print(x)

