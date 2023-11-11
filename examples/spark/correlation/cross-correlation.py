#!/usr/bin/env python3

# Does not work fully as it considers pairs like (shoes, bags) and (bags, shoes)
# to be different but they should be the same.


def printRDD(rdd):
    result = rdd.collect()
    for x in result:
        print(x)
    print()

import os
import sys
import itertools
from itertools import combinations

if (len(sys.argv) != 2):
    print("Usage: cross-correlation.py <input folder>")
    sys.exit(1)

from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('Cross Correlation')
sc = SparkContext.getOrCreate(conf=conf)

rdd = sc.wholeTextFiles(sys.argv[1])

rdd1 = rdd.flatMap(lambda file: [d.split() for d in file[1].split('\n') if d!=''])
#rdd2 = rdd1.map(lambda file: (x,y) for x,y in file )
print("---- Step 1 ----")
printRDD(rdd1)


rdd2 = rdd1.map(lambda x: list(combinations(x, 2))).flatMap(lambda x: x)
print("---- Step 2 ----")
printRDD(rdd2)

rdd3 = rdd2.map(lambda x: (x,1)).groupByKey().mapValues(lambda vals:len(vals))
print("---- Step 3 ----")
printRDD(rdd3)

rdd3.sortBy(lambda x: x[1], ascending = False)
print("---- Step 4 ----")
printRDD(rdd3)

