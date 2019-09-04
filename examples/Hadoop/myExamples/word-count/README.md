
#Building and running Hadoop MapReduce jobs

##To build jar file manually:

```
export JAVA_HOME=/usr/java/default
export PATH=${JAVA_HOME}/bin:${PATH}
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar

We assume that hadoop is in your PATH.

cd src/
hadoop com.sun.tools.javac.Main WordCount.java
jar cf ../wc.jar WordCount*.class
rm -f *.class
cd ..
```

##To build jar file in Eclipse:

Create a normal Java project and add the following external jar files (adjusting paths to match your
paths):

```
hadoop/hadoop-2.9.2/share/hadoop/common/hadoop-common-2.9.2.jar
hadoop/hadoop-2.9.2/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.9.2.jar
hadoop/hadoop-2.9.2/share/hadoop/hdfs/lib/commons-cli-1.2.jar
```

Then export project as jar file and you should be set.

##To run:

Make sure you have HDFS running either in standalone or pseudo-distributed mode before doing
the following steps. See class notes for more details.

```
hdfs  dfs -put input
hadoop jar wc.jar input output
hdfs dfs -get output
```


