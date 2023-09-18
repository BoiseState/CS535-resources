

#Building and running Hadoop MapReduce jobs in Python

Test it locally first:

echo "the the and me and me the" | ./mapper.py | sort | ./reducer.py


##To run:

Make sure you have HDFS running either in standalone or pseudo-distributed mode before doing
the following steps. See class notes for more details.

```
hdfs  dfs -put ../input
 
hadoop jar ~/hadoop-install/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar -mapper mapper.py -reducer reducer.py -input input -output output -file ./mapper.py -file ./reducer.py

hdfs dfs -get output
```





