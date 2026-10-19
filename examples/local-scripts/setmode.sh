#!/bin/bash
# author: Amit Jain

HADOOP_HOME=$HOME/hadoop-install/hadoop

if test ! -d "${HADOOP_HOME}"
then
	echo
	echo "Error: missing hadoop install folder: ${HADOOP_HOME}"
	echo "Install hadoop in install folder before running this script!"
	echo
	exit 1
fi

case $# in
0) echo "Usage: $0 <standalone|pseudo-distributed|distributed>"; exit 1;;
esac

cd templates/

if test "${1:0:1}" == "s"
then
	echo "Setting up config files for standalone mode."
	cp core-site.xml.standalone ${HADOOP_HOME}/etc/hadoop/core-site.xml
	cp hdfs-site.xml.standalone ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml
	cp mapred-site.xml.standalone ${HADOOP_HOME}/etc/hadoop/mapred-site.xml
	cp masters.standalone ${HADOOP_HOME}/etc/hadoop/masters
	cp workers.standalone ${HADOOP_HOME}/etc/hadoop/workers
	exit
fi

if test "${1:0:1}" == "p"
then
	echo "Setting up config files for pseudo-distributed mode."
	cp core-site.xml.pseudo-distributed ${HADOOP_HOME}/etc/hadoop/core-site.xml
	cp yarn-site.xml.pseudo-distributed ${HADOOP_HOME}/etc/hadoop/yarn-site.xml
	cp hdfs-site.xml.pseudo-distributed ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml
	cp mapred-site.xml.pseudo-distributed ${HADOOP_HOME}/etc/hadoop/mapred-site.xml
	cp masters.pseudo-distributed ${HADOOP_HOME}/etc/hadoop/masters
	cp workers.pseudo-distributed ${HADOOP_HOME}/etc/hadoop/workers
	exit
fi

if test "${1:0:1}" == "d"
then
	master=$(hostname)
	echo "Setting up config files for distributed mode (assuming $master as master node)."
	echo "Make sure to update the ${HADOOP_HOME}/etc/hadoop/workers files in the hadoop install folder with your nodes"
	cp core-site.xml.distributed ${HADOOP_HOME}/etc/hadoop/core-site.xml
	cp hdfs-site.xml.distributed ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml
	cp mapred-site.xml.distributed ${HADOOP_HOME}/etc/hadoop/mapred-site.xml
	cp yarn-site.xml.distributed ${HADOOP_HOME}/etc/hadoop/yarn-site.xml
	cp masters.distributed ${HADOOP_HOME}/etc/hadoop/masters
	cp workers.pseudo-distributed ${HADOOP_HOME}/etc/hadoop/workers
	#
	sed 's/YOURUSERNAME/'"$(whoami)"'/' ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml > temp.$$
	mv temp.$$ ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml
	#
	sed 's/YOURUSERNAME/'"$(whoami)"'/' ${HADOOP_HOME}/etc/hadoop/mapred-site.xml > temp.$$
	mv temp.$$ ${HADOOP_HOME}/etc/hadoop/mapred-site.xml
	#
	sed 's/MASTERNODE/'$master'/' ${HADOOP_HOME}/etc/hadoop/core-site.xml > temp.$$
	mv temp.$$ ${HADOOP_HOME}/etc/hadoop/core-site.xml
	#
	sed 's/MASTERNODE/'$master'/' ${HADOOP_HOME}/etc/hadoop/yarn-site.xml > temp.$$
	mv temp.$$ ${HADOOP_HOME}/etc/hadoop/yarn-site.xml
	#
	echo $master > ${HADOOP_HOME}/etc/hadoop/masters
	exit
fi

echo $0": Unknown option...try again"
