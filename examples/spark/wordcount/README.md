
Build
=====

Build the jar file directly in Eclipse (just include all Spark jar files from
~/spark-install/spark/jars as External jars for the project.

Run
===

spark-submit --class "WordCount" --master local[4] wordcount.jar input

where local[4] says to use 4 threads on local machine. You can change that to higher or lower
or replace by * to use as many threads as the number of logical threads on your local system.

For a Spark cluster that is up and running but using shared local filesystem , use:

spark-submit --class WordCount --master spark://cscluster00.boisestate.edu:7077 wordcount-basic.jar input


For a Spark cluster that is up and running along with a Hadoop cluster, use:

spark-submit --class WordCount --master spark://cscluster00.boisestate.edu:7077 wordcount-hdfs-save.jar hdfs://cscluster00:9000/user/amit/input




Logging
=======

By default, spark generates a lot of info messages. You can redirect them to a file as follows:

spark-submit --class "WordCount" --master local[4]  wordcount.jar input  2> log

Or you can control the logging level from your program. Here is the relevant code:

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

. . .

Logger log = LogManager.getRootLogger();
log.setLevel(Level.WARN);

