
Build
=====

Build the jar file directly in Eclipse (just include all Spark jar files from
~/spark-install/spark/jars as External jars for the project.

Run
===

spark-submit --class "SimpleApp" --master local[4] simpleapp.jar

where local[4] says to use 4 threads on local machine. You can change that to higher or lower
or replace by * to use as many threads as the number of logical threads on your local system. We can
skip the class file if the jar file has the main class specified when it was built.

spark-submit --master local[4] simpleapp.jar

There are a lot of info messages. You can redirect them to a file as follows:

spark-submit --class "SimpleApp" --master local[4] simpleapp.jar 2> log

Or you can control the logging level from your program. See SimpleApp2.java for an example. Here is
the relevant code:


import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

. . .

Logger log = LogManager.getRootLogger();
log.setLevel(Level.WARN);


You can also configure Spark defaults by creating a spark/conf/log4j.properties file. This is
a bit more complex but would make more sense in a production environment.

To run the second example, use the appropriate class name as follows:

spark-submit --class "SimpleApp2" --master local[4] simpleapp.jar 2> log

