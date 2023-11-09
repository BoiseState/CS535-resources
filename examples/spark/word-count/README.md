

# Run

## Java
Build the jar file directly in Eclipse (just include all Spark jar files from
~/spark-install/spark/jars as External jars for the project.

spark-submit --class "WordCount" --master local[4] spark-wc.jar input output

where local[4] says to use 4 threads on local machine. You can change that to higher or lower
or replace by * to use as many threads as the number of logical threads on your local system.

For a Spark cluster that is up and running but using shared local filesystem , use:

spark-submit --class WordCount --master spark://cscluster00.boisestate.edu:7077 spark-wc.jar input output

For a Spark cluster that is up and running along with a Hadoop cluster, use:

spark-submit --class WordCount --master spark://cscluster00.boisestate.edu:7077 spark-wc.jar hdfs://cscluster00:9000/user/amit/input hdfs://cscluster00:900/user/amit/output

Replace amit with your user name on the cluster.

## Python

spark-submit --master local[4] wordcount.py input output

where local[4] says to use 4 threads on local machine. You can change that to higher or lower
or replace by * to use as many threads as the number of logical threads on your local system.

For a Spark cluster that is up and running but using shared local filesystem , use:

spark-submit --master spark://cscluster00.boisestate.edu:7077 wordcount.py input output

For a Spark cluster that is up and running along with a Hadoop cluster, use:

spark-submit --master spark://cscluster00:7077 wordcount.py hdfs://cscluster00:9000/user/amit/input hdfs://cscluster00:9000/user/amit/output  

Replace amit with your user name on the cluster.


# Logging

By default, spark generates a lot of info messages. You can redirect them to a file as follows:

spark-submit --class "WordCount" --master local[4]  wordcount.jar input  2> log

Or you can control the logging level from your program.

