

#Building and running streaming Hadoop MapReduce jobs with Java

Test it locally first:

echo "the the and me and me the" | java Mapper | sort | java Reducer


##To run:

Make sure you have HDFS running either in standalone or pseudo-distributed mode before doing
the following steps. See class notes for more details.

For this to work, I had to increase the max heap size  to 4000m in ~/hadoop-install/hadoop/etc/hadoop.yarn-env.sh

JAVA_HEAP_MAX=-Xmx4000m


```
hdfs  dfs -put ../input
 
hadoop jar ~/hadoop-install/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar -mapper " java Mapper" -reducer "java Reducer"  -input input -output output -file ./Mapper.class  -file ./Reducer.class

hdfs dfs -get output
```






