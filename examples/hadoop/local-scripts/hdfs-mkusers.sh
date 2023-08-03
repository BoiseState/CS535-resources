#!/bin/bash

cd /home
for name in * 
do 
	userName=$(echo $name | tr '[:upper:]' '[:lower:]';)
	echo $userName
	hdfs dfs -mkdir /user/$userName
	hdfs dfs -chmod 700 /user/$userName
	hdfs dfs -chown $userName /user/$userName
	hdfs dfs -ls -d /user/$userName
done

echo "setting up staging folders in DFS"

hdfs dfs -mkdir /tmp
hdfs dfs -chmod 1777 /tmp
hdfs dfs -mkdir /tmp/hadoop-yarn
hdfs dfs -chmod 777 /tmp/hadoop-yarn
hdfs dfs -mkdir /tmp/hadoop-yarn/staging
hdfs dfs -chmod 777 /tmp/hadoop-yarn/staging
hdfs dfs -mkdir /tmp/hadoop-yarn/staging/history
hdfs dfs -chmod 777 /tmp/hadoop-yarn/staging/history

