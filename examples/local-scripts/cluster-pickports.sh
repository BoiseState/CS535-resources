#!/bin/sh

# This script modifies ports in config files to allow multiple instances of hadoop to 
# run on the same cluster using common namenode but separate datanodes.
# author: Marissa Hollingsworth and Amit Jain

HADOOP_HOME=$HOME/hadoop-install/hadoop

if test ! -d "${HADOOP_HOME}"
then
    echo
    echo "Error: missing hadoop install folder: ${HADOOP_HOME}"
    echo "Install hadoop in install folder before running this script!"
    echo
    exit 1
fi

if [ $# -ne 1 ]
then
    echo "Usage: $0 <base port>"
	exit 1
fi

baseport=$1
if [ $baseport -gt  62000 -o $baseport -lt 10000 ]
then
	echo "$0: bad base port range: choose between 10000 and 60000"
	exit 1
fi
if [ $baseport -eq  50000 ]
then
	echo "$0: forbidden base port range: 50000 is standard port so avoid it!"
	exit 1
fi

cd ${HADOOP_HOME}

/bin/cp etc/hadoop/core-site.xml etc/hadoop/core-site.xml.backup
/bin/cp etc/hadoop/hdfs-site.xml etc/hadoop/hdfs-site.xml.backup
/bin/cp etc/hadoop/mapred-site.xml etc/hadoop/mapred-site.xml.backup
/bin/cp etc/hadoop/yarn-site.xml etc/hadoop/yarn-site.xml.backup


sed -i "s/YOURUSERNAME/`whoami`/" etc/hadoop/hdfs-site.xml
sed -i "s/YOURUSERNAME/`whoami`/" etc/hadoop/mapred-site.xml

port=$baseport
param="fs.default.name"
newvalue="<value>hdfs://node00:$port</value>"
sed -i "/$param/ {
n
c\
$newvalue
}" etc/hadoop/core-site.xml


port=$[baseport+10]
param="dfs.datanode.address"
newvalue="<value>0.0.0.0:$port</value>"
sed -i "/$param/ {
n
c\
$newvalue
}" etc/hadoop/hdfs-site.xml

port=$[baseport+70]
param="dfs.http.address"
newvalue="<value>0.0.0.0:$port</value>"
sed -i "/$param/ {
n
c\
$newvalue
}" etc/hadoop/hdfs-site.xml

port=$[baseport+91]
param="dfs.secondary.http.address"
newvalue="<value>0.0.0.0:$port</value>"
sed -i "/$param/ {
n
c\
$newvalue
}" etc/hadoop/hdfs-site.xml

port=$[baseport+75]
param="dfs.datanode.http.address"
newvalue="<value>0.0.0.0:$port</value>"
sed -i "/$param/ {
n
c\
$newvalue
}" etc/hadoop/hdfs-site.xml


port=$[baseport+20]
param="dfs.datanode.ipc.address"
newvalue="<value>0.0.0.0:$port</value>"
sed -i "/$param/ {
n
c\
$newvalue
}" etc/hadoop/hdfs-site.xml

param="<name>yarn.resourcemanager.hostname"
newvalue="<value>192.168.0.1</value>"
sed -i "/$param/ {
n
c\
$newvalue
}" etc/hadoop/yarn-site.xml

port=$[baseport+41]
param="yarn.resourcemanager.address"
newvalue="<value>\${yarn.resourcemanager.hostname}:$port</value>"
sed -i "/$param/ {
n
c\
$newvalue
}" etc/hadoop/yarn-site.xml


port=$[baseport+42]
param="yarn.resourcemanager.scheduler.address"
newvalue="<value>\${yarn.resourcemanager.hostname}:$port</value>"
sed -i "/$param/ {
n
c\
$newvalue
}" etc/hadoop/yarn-site.xml

port=$[baseport+88]
param="yarn.resourcemanager.webapp.address"
newvalue="<value>0.0.0.0:$port</value>"
sed -i "/$param/ {
n
c\
$newvalue
}" etc/hadoop/yarn-site.xml

port=$[baseport+90]
param="yarn.resourcemanager.webapp.https.address"
newvalue="<value>\${yarn.resourcemanager.hostname}:$port</value>"
sed -i "/$param/ {
n
c\
$newvalue
}" etc/hadoop/yarn-site.xml

port=$[baseport+31]
param="yarn.resourcemanager.resource-tracker.address"
newvalue="<value>\${yarn.resourcemanager.hostname}:$port</value>"
sed -i "/$param/ {
n
c\
$newvalue
}" etc/hadoop/yarn-site.xml


port=$[baseport+33]
param="yarn.resourcemanager.admin.address"
newvalue="<value>\${yarn.resourcemanager.hostname}:$port</value>"
sed -i "/$param/ {
n
c\
$newvalue
}" etc/hadoop/yarn-site.xml



port=$[baseport+7]
param="yarn.nodemanager.address"
newvalue="<value>0.0.0.0:$port</value>"
sed -i "/$param/ {
n
c\
$newvalue
}" etc/hadoop/yarn-site.xml

port=$[baseport+8]
param="yarn.timeline-service.address"
newvalue="<value>0.0.0.0:$port</value>"
sed -i "/$param/ {
n
c\
$newvalue
}" etc/hadoop/yarn-site.xml

port=$[baseport+9]
param="yarn.timeline-service.webapp.address"
newvalue="<value>0.0.0.0:$port</value>"
sed -i "/$param/ {
n
c\
$newvalue
}" etc/hadoop/yarn-site.xml

port=$[baseport+12]
param="yarn.timeline-service.webapp.https.address"
newvalue="<value>0.0.0.0:$port</value>"
sed -i "/$param/ {
n
c\
$newvalue
}" etc/hadoop/yarn-site.xml

port=$[baseport+11]
param="yarn.log.server.url"
newvalue="<value>0.0.0.0:$port</value>"
sed -i "/$param/ {
n
c\
$newvalue
}" etc/hadoop/yarn-site.xml

                
port=$[baseport+40]
param="yarn.nodemanager.localizer.address"
newvalue="<value>0.0.0.0:$port</value>"
sed -i "/$param/ {
n
c\
$newvalue
}" etc/hadoop/yarn-site.xml

port=$[baseport+44]
param="yarn.nodemanager.webapp.address"
newvalue="<value>0.0.0.0:$port</value>"
sed -i "/$param/ {
n
c\
$newvalue
}" etc/hadoop/yarn-site.xml


port=$[baseport+47]
param="yarn.sharedcache.admin.address"
newvalue="<value>\${yarn.resourcemanager.hostname}:$port</value>"
sed -i "/$param/ {
n
c\
$newvalue
}" etc/hadoop/yarn-site.xml

port=$[baseport+46]
param="yarn.sharedcache.uploader.server.address"
newvalue="<value>\${yarn.resourcemanager.hostname}:$port</value>"
sed -i "/$param/ {
n
c\
$newvalue
}" etc/hadoop/yarn-site.xml

port=$[baseport+45]
param="yarn.sharedcache.client-server.address"
newvalue="<value>\${yarn.resourcemanager.hostname}:$port</value>"
sed -i "/$param/ {
n
c\
$newvalue
}" etc/hadoop/yarn-site.xml

echo
echo "Updated files yarn-site.xml, core-site.xml, hdfs-site.xml, mapred-site.xml in hadoop etc/hadoop folder"
echo

