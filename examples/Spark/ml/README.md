
Build
=====

Build the jar file directly in Eclipse (just include all Spark jar files from
~/spark-install/spark/jars as External jars for the project.

Run
===

spark-submit --class "JavaCorrelationsExample" --master local[4] java-ml-correlations.jar input

where local[4] says to use 4 threads on local machine. You can change that to higher or lower
or replace by * to use as many threads as the number of logical threads on your local system.


Logging
=======

By default, spark generates a lot of info messages. You can redirect them to a file as follows:

spark-submit --class "JavaCorrelationExamples" --master local[4]  java-ml-correlations.jar 2> log

Or you can control the logging level from your program. Here is the relevant code:

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

. . .

Logger log = LogManager.getRootLogger();
log.setLevel(Level.WARN);

