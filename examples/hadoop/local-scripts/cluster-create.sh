#!/bin/sh
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

cd ${HADOOP_HOME}


# check to see that the etc/hadoop/workers  matches the nodes allocated to the user
/bin/rm -fr logs pids
mkdir {logs,pids}

pdsh -w - < etc/hadoop/workers  /bin/rm -fr /tmp/hadoop-`whoami`
pdsh -w - < etc/hadoop/workers  mkdir /tmp/hadoop-`whoami`
pdsh -w - < etc/hadoop/workers  chmod 700 /tmp/hadoop-`whoami`

cd etc/hadoop
for node in $(cat workers)
do
    mkdir -p ../pids/$node
done

mkdir -p ../pids/`hostname`
cd ../..

echo "Formatting the DFS filesystem"
bin/hdfs namenode -format

echo "Starting the Hadoop runtime system on all nodes"
sbin/start-dfs.sh
sbin/start-yarn.sh
