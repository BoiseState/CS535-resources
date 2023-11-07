#!/usr/bin/env python3
# coding: utf-8

import os
import sys
import itertools
import findspark
import pyspark
from pyspark.sql import SparkSession

if (len(sys.argv) != 2):
    print("Usage: top-n-patents.py <input folder>")
    sys.exit(1)

spark = SparkSession.builder.master('local[*]').appName("case-analysis").getOrCreate()
sc = spark.sparkContext

rdd = sc.wholeTextFiles(sys.argv[1])

# Find the number of citations for each patent in a patent reference data set. The format of
# the input is citing_patent, cited_patent

rdd1 = rdd.flatMap(lambda file : [tuple(reversed(d.split())) for d in file[1].split('\n') if d != ''])
rdd2 = rdd1.groupByKey().mapValues(lambda vals : len(vals))

# Find the top N most frequently cited patents. The format of the input is
# citing_patent, cited_patent

rdd3 = rdd2.sortBy(lambda x: x[1], ascending = False)

result = rdd3.collect()
for value in result:
    print(value)

