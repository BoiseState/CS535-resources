
#Building and running Hadoop MapReduce jobs

##To build jar file manually:

```
export JAVA_HOME=/usr/java/default
export PATH=${JAVA_HOME}/bin:${PATH}
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar

bin/hadoop com.sun.tools.javac.Main CaseAnalysis.java
jar cf case-analysis.jar CaseAnalysis*.class
```

##To build jar file in Eclipse:

Create a normal Java project and add the following external jar files (adjusting paths to match your
paths):

```
hadoop/hadoop-2.10.1/share/hadoop/common/hadoop-common-2.10.1.jar
hadoop/hadoop-2.10.1/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.10.1.jar
hadoop/hadoop-2.10.1/share/hadoop/hdfs/lib/commons-cli-1.2.jar
```

Then export project as jar file and you should be set.

##To run:

Make sure you have HDFS running either in standalone or pseudo-distributed mode before doing
the following steps. See Hadoop wiki for more info here:
(http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html)

```
hdfs  dfs -put input
hadoop jar case-analysis.jar input output
hdfs dfs -get output
```

If you created the jar file with Eclipse (and set the main class), then run it as follows:

```
hadoop jar case-analysis.jar input output
```

