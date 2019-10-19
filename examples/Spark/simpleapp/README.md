
Build
=====

mvn clean
mvn package

Run
===

spark-submit --class "SimpleApp" --master local[4] target/simple-project-1.0.jar

where local[4] says to use 4 threads on local machine. You can change that to higher or lower
or replace by * to use as many threads as the number of logical threads on your local system.
