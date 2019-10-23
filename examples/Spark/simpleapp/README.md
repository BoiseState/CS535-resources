
Build
=====

mvn clean
mvn package

or build the jar file directly in Eclipse (just include all Spark jar files from
~/spark-install/spark/jars as External jars for the project.

Run
===

spark-submit --class "SimpleApp" --master local[4] target/simple-project-1.0.jar

where local[4] says to use 4 threads on local machine. You can change that to higher or lower
or replace by * to use as many threads as the number of logical threads on your local system.

There are a lot of info messages. You can redirect them to a file as follows:

spark-submit --class "SimpleApp" --master local[4] target/simple-project-1.0.jar 2> log

Or you can control the logging level from your program. See SimpleApp2.java for an example. Here is
the relevant code:


import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

. . .

Logger log = LogManager.getRootLogger();
log.setLevel(Level.WARN);


You can also configure Spark defaults by creating a spark/conf/log4j.properties file. This is
a bit more complex but would make more sense in a production environment.
