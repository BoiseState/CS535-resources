

# Run

This README deals with running on your local system - modify as needed to run on a cluster.

## Java version

Build the jar file directly in Eclipse (just include all Spark jar files from
~/spark-install/spark/jars as External jars for the project).

spark-submit --class "CaseAnalysis" --master local[4] case-analysis.jar input

where local[4] says to use 4 threads on local machine. You can change that to higher or lower
or replace by * to use as many threads as the number of logical threads on your local system.

For input files on HDFS, make sure Hadoop is up and running. Then use the following 

spark-submit --class "CaseAnalysis" --master local[4] case-analysis.jar hdfs://localhost:9000/user/amit/input


## Python version
spark-submit --master local[4] case-analysis.py input

where local[4] says to use 4 threads on local machine. You can change that to higher or lower
or replace by * to use as many threads as the number of logical threads on your local system.

For input files on HDFS, make sure Hadoop is up and running. Then use the following 

spark-submit --master local[4] case-analysis.py hdfs://localhost:9000/user/amit/input


# Logging

By default, spark generates a lot of info messages. You can redirect them to a file as follows:

spark-submit --class "CaseAnalysis" --master local[4]  case-analysis.jar input  2> log

Or you can control the logging level from your program. 

