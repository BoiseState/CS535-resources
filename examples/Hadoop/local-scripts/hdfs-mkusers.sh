#!/bin/bash

cd /home
for name in * 
do 
	userName=$(echo $name | tr '[:upper:]' '[:lower:]';)
	echo $userName
	hdfs dfs -mkdir /user/$userName
	hdfs dfs -chmod 700 /user/$userName
	hdfs dfs -ls -d /user/$userName
done
