#!/bin/bash


case $# in
0) echo "Usage: `basename $0` <users file, one per line>"; exit 1;;
esac

users=$1

echo "hdfs-mkusers: Using list of users from file: " $users

for name in $(cat $users) 
do 
	userName=$(echo $name | tr '[:upper:]' '[:lower:]';)
	echo $userName
	hdfs dfs -mkdir /user/$userName
	hdfs dfs -chmod 700 /user/$userName
	hdfs dfs -chown $userName /user/$userName
	hdfs dfs -ls -d /user/$userName
done

echo
echo "Make sure to also run hdfs-setup-tmp.sh to complete multi-user setup!"
echo

