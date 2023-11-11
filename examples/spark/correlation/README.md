
# Build

Build the jar file directly in Eclipse (just include all Spark jar files from
~/spark-install/spark/jars as External jars for the project.

# Java

spark-submit --class "CrossCorrelation" --master local[4] cross-correlation.jar input

where local[4] says to use 4 threads on local machine. You can change that to higher or lower
or replace by * to use as many threads as the number of logical threads on your local system.

# Python

The python version **does not work fully** as it considers pairs like (shoes, bags) and (bags, shoes)
to be different but they should be the same.

spark-submit --master local[4] cross-correlation.py input

where local[4] says to use 4 threads on local machine. You can change that to higher or lower
or replace by * to use as many threads as the number of logical threads on your local system.

# Logging

By default, spark generates a lot of info messages. You can redirect them to a file as follows:

spark-submit --class "CrossCorrelation" --master local[4]  cross-correlation.jar input  2> log

Or you can control the logging level from your program. 


