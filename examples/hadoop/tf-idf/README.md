
#Building and running Hadoop MapReduce jobs

##To build jar file manually:


We assume that hadoop is in your PATH.
```
cd src/
hadoop com.sun.tools.javac.Main TfIdfDriver.java
jar cf TfIdfDriver ../tf-idf.jar *.class
rm -f *.class
cd ..
```

##To build jar file in Eclipse:

Create a normal Java project and add the following external jar files (adjusting paths to match
your paths):

```
~/hadoop-install/hadoop/hadoop-3.3.6/share/hadoop/common/hadoop-common-3.3.6.jar
~/hadoop-install/hadoop/hadoop-3.3.6/share/hadoop/mapreduce/hadoop-mapreduce-client-core-3.3.6.jar
~/hadoop-install/hadoop/hadoop-3.3.6/share/hadoop/hdfs/lib/commons-cli-1.2.jar
```

Then export project as jar file and you should be set.

##To run:

Make sure you have HDFS running either in standalone or pseudo-distributed mode before doing
the following steps. See class notes for more details.

```
hdfs  dfs -put ../word-count/input
hadoop jar tf-idf.jar input output
hdfs dfs -get output
```


