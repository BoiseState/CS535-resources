#!/usr/bin/env python3
# coding: utf-8

import os
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('Homework').setMaster('local[*]')
sc = SparkContext.getOrCreate(conf=conf)

import itertools
path = "input/"
rdd = sc.wholeTextFiles(path)

# Find the number of citations for each patent in a patent reference data set. The format of
# the input is citing_patent, cited_patent

rdd1 = rdd.flatMap(lambda file: [tuple(reversed(d.split())) for d in file[1].split('\n') if d!=''])
rdd2 = rdd1.groupByKey().mapValues(lambda vals:len(vals))
rdd2.collect()

# Find the top N most frequently cited patents. The format of the input is
# citing_patent, cited_patent

rdd2.sortBy(lambda x: x[1], ascending = False).collect()

result = rdd2.collect()
for value in result:
    print(value)

