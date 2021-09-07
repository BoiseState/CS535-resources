

#Building and running Hadoop MapReduce jobs in Python

Test it locally first:

echo "the the and me and me the" | mapper.py | sort | reducer.py


##To run:

Make sure you have HDFS running either in standalone or pseudo-distributed mode before doing
the following steps. See class notes for more details.

```
hdfs  dfs -put ../input

hadoop jar contrib/streaming/hadoop-*streaming*.jar \
-file mapper.py    -mapper mapper.py \
-file reducer.py   -reducer reducer.py \
-input input/* -output output

hdfs dfs -get output
```



