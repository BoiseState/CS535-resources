#!/usr/bin/env python
# coding: utf-8

# # Word count using Data Frames

from pyspark.sql import SparkSession, Row
usingfrom pyspark.sql.functions import *

spark = (SparkSession
        .builder
        .appName("Word Count")
        .getOrCreate())

lines = spark.read.text("../word-count/input/*.txt")

lines.printSchema()

words = lines.withColumn("words", explode(split(trim(col("value")), " ")))

words.show(100)

wordcounts = words.groupBy("words").count()

wordcounts.printSchema()

wordcounts.show(10)

wordcounts.sort(col("count"), ascending=False).show(10)

wordcounts.filter(col("words") == "Rabbit").show(10)

spark.stop()
