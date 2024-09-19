import pyspark
from pyspark.storagelevel import StorageLevel
from pyspark.sql import (
    functions as F, 
    types as T,
    SparkSession,
)
import os

spark = (
    SparkSession.builder
    .appName("My Cool Spark Program")
    .getOrCreate()
)

(
    spark.read
    # Refers to the local file system
    .text("s3://bsu-c535-fall2024-commons/datasets/shakespeare/")
    .createOrReplaceTempView("willy")
)

# Word count for the William Shakespeare dataset
(
    spark.table("willy")
    .withColumn("words", F.split(F.col("value"), r'\s+'))
    .select(F.explode("words").alias("word"))
    .withColumn(
        "new_word",
        # we don't want things like capitalization or punctuation to 
        # affect the word count.
        F.lower(F.regexp_replace("word", r"[^A-Za-z]", "")),
    )
    .filter(F.col("new_word") != "")
    .groupby("new_word")
    .agg(F.count("*").alias("frequency"))
    .write.mode("OVERWRITE").parquet(os.environ['PAGE_PAIRS_OUTPUT'])
)