

# Run

## Java
Build the jar file directly in Eclipse (just include all Spark jar files from
~/spark-install/spark/jars as External jars for the project.

spark-submit --class "SparkPlayground" --master local[4] spark-playground.jar 

where local[4] says to use 4 threads on local machine. You can change that to higher or lower
or replace by * to use as many threads as the number of logical threads on your local system.

For a Spark cluster that is up and running but using shared local filesystem , use:

spark-submit --class SparkPlayground --master spark://cscluster00.boisestate.edu:7077 spark-playground.jar 


## Python

spark-submit --master local[4] wordcount.py input output

where local[4] says to use 4 threads on local machine. You can change that to higher or lower
or replace by * to use as many threads as the number of logical threads on your local system.

For a Spark cluster that is up and running but using shared local filesystem , use:

spark-submit --master spark://cscluster00.boisestate.edu:7077 rdd-playground.py 

For a Spark cluster that is up and running along with a Hadoop cluster, use:

spark-submit --master spark://cscluster00.boisestate.edu:7077 rdd-playground.py 


# Logging

By default, spark generates a lot of info messages. You can redirect them to a file as follows:

spark-submit --class "SparkPlayground" --master local[4]  spark-playground.jar input  2> log

Or you can control the logging level from your program.

