from pyspark.sql import (
    SparkSession,
    functions as F,
    types as T
)
import pyspark
from pyspark import StorageLevel
import datetime

spark = (
    SparkSession.builder
    .remote("sc://localhost:15002")
    .appName("spark_examples")
    .getOrCreate()
)

def parquet(prefix):
    files = spark.read.parquet(f"s3://bsu-c535-fall2024-commons/arjun-workspace/{prefix}/")
    files.save(prefix)

def save(self, name):
    t = datetime.datetime.now()
    (
        self
        # We use the (serialized) memory and disk storage level without
        # replication. Replication helps us in long-running processes if
        # one our nodes in the cluster dies.
        .persist(StorageLevel.MEMORY_AND_DISK)
        .createOrReplaceTempView(name)
    )
    rows = spark.table(name).count()
    print(f"{name} - {rows} rows - elapsed {datetime.datetime.now() - t}")
    spark.table(name).printSchema()

# for interactive EMR through Spark Connect
pyspark.sql.connect.dataframe.DataFrame.save = save
# use this when you're submitting steps to EMR
# pyspark.sql.DataFrame.save = save

parquet("linktarget")
parquet("page")

(
    spark.table("linktarget")
    .join(
        spark.table("page"), 
        F.expr("lt_title = page_title and page_namespace = lt_namespace"),
        # This join gives us the page IDs for every link target.
        "inner",
        # A left join gives us the page ID for every link target, if it's a
        # page. Otherwise, all column in the page table are set to null..
        # "left",
        # A right join gives us the link target for every page, if the page
        # is a link target. If the page is not a link target, the link target
        # columns are set to null.
        # "right",
    )
    .save("lt_page")
)