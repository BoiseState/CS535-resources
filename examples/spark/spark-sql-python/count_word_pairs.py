from pyspark.sql import (
    SparkSession,
    functions as F,
    types as T
)

spark = (
    SparkSession.builder
    .remote("sc://localhost:15002")
    .appName("spark_examples")
    .getOrCreate()
)

# Question 2: for every pair of words, I want to know how often they
# occur in the same line

# Method 1: Much like our "count total length for words that start with A"
# example, we can use monotonically_increasing_id(), and explode twice
# to produce the pairs
(
    spark.table("willy")
    .withColumn("words", F.split(F.col("value"), r'\s+'))
    .withColumn("row_id", F.monotonically_increasing_id())
    .select(F.explode("words").alias("word"), "row_id")
    .withColumn(
        "new_word",
        F.lower(F.regexp_replace("word", r"[^A-Za-z]", ""))
    )
    .filter(F.expr("new_word is not null and new_word != ''"))
    .groupby("row_id")
    .agg(F.collect_set("new_word").alias("unique_words"))
    # exploding twice creates the pairs
    .selectExpr("unique_words", "explode(unique_words) pair_2")
    .selectExpr("explode(unique_words) pair_1", "pair_2")
    # we only want one of each pair, not two
    # we also don't want self-pairs
    .filter("pair_1 > pair_2")
    .groupby("pair_1", "pair_2")
    .agg(F.count("*").alias("frequency"))
    .show(truncate=False)
)

# Method 2: a more efficient row-wise solution with functional programming.
# It works like a double-nested for loop to produce the pairs.
(
    spark.table("willy")
    .withColumn("words", F.split(F.col("value"), r'\s+'))
    .withColumn("new_words",
        F.expr("transform(words, x -> lower(regexp_replace(x, '[^A-Za-z]', '')))")
    )
    .withColumn("unique_words", F.expr("array_distinct(new_words)"))
    .withColumn("pre_pairs",
        F.expr("""transform(unique_words, 
        x -> struct(x as pair_1, unique_words as pair_2))""")
    )
    .withColumn("word_pairs", F.expr("""
        flatten(transform(pre_pairs, x -> 
            transform(x.pair_2, 
                y -> struct(x.pair_1 as pair_1, y as pair_2)
            )
        ))
    """))
    .withColumn("uniq_word_pairs", F.expr("""
        filter(word_pairs, x -> x.pair_1 > x.pair_2)
    """))
    .selectExpr("explode(uniq_word_pairs) as pair")
    .groupby("pair")
    .agg(F.count("*"))
    .show(20, truncate=False, vertical=True)
)