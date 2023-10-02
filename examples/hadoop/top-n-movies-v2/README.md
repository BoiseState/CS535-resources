
#Building and running Hadoop MapReduce jobs

Note that this solution assumes that another pass has already been done so we have movies along with
their view counts available. This would typically be the first pass of the MapReduce solution that
starts with view data based on user ids.

##To build jar file manually:

We assume that hadoop is in your PATH.
```
hadoop com.sun.tools.javac.Main TopMoviesMapper.java TopMoviesReducer.java TopNMoviesDriver.java
jar cfe topn-v2.jar TopNMoviesDriver *.class
```

##To build jar file in Eclipse:

Create a normal Java project and add the following external jar files (adjusting paths to match your
paths):

```
~/hadoop-install/hadoop-3.3./share/hadoop/common/hadoop-common-3.3..jar
~/hadoop-install/hadoop-3.3./share/hadoop/mapreduce/hadoop-mapreduce-client-core-3.3..jar
~/hadoop-install/hadoop-3.3./share/hadoop/hdfs/lib/commons-cli-1.2.jar
```

Then export project as jar file and you should be set.

##To run:

Make sure you have HDFS running either in standalone or pseudo-distributed mode before doing
the following steps. See class notes for more details.

```
hdfs  dfs -put input
hadoop jar topn-v2.jar input output
hdfs dfs -get output
```


