
Build
=====

The folder needs to have the following layout:

build.sbt
src/main/scala/SimpleApp.scala

Then we can compile and build a jar file as follows:

sbt compile 
sbt package

The jar file will be in the folder target/scala2.12/

Run
===

spark-submit --class "SSSPExample" --master local[4] target/scala-2.12/shortest-paths_2.12-1.0.jar


where local[4] says to use 4 threads on local machine. You can change that to higher or lower
or replace by * to use as many threads as the number of logical threads on your local system. We
can skip the class file if the jar file has the main class specified when it was built.

spark-submit --master local[4] target/scala-2.12/shortest-paths_2.12-1.0.jar

There are a lot of info messages. You can redirect them to a file as follows:

spark-submit --class "SSSPExample" --master local[4] target/scala-2.12/shortest-paths_2.12-1.0.jar
2> log

