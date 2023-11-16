
Build
=====

Build the jar file directly in Eclipse (just include all Spark jar files from
~/spark-install/spark/jars as External jars for the project.

Run
===

spark-submit --class "JavaSparkSQLExample1" --master local[4] java-sql-example-1.jar

where local[4] says to use 4 threads on local machine. You can change that to higher or lower
or replace by * to use as many threads as the number of logical threads on your local system.


Logging
=======

By default, spark generates a lot of info messages. You can redirect them to a file as follows:

spark-submit --class "JavaSparkSQLExample1" --master local[4]  java-sql-example-1.jar  2> log

Or you can control the logging level from your program. 


