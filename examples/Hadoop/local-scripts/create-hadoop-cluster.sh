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


# Check to see if any nodes are allocated by the user currently
# and that only one job is running
status=`qstat -1n | grep ${USER:0:8} | awk '{print $10}'`
count=`qstat -1n | grep ${USER:0:8} | awk '{print $10}'| wc -l`
if test "$count" == "0" 
then
	echo "Please first reserve nodes via pbsget in a separate window!!!"
	exit 1
fi
if test "$count" != "1" 
then
	echo "Only one job can be running on the cluster to be able to run Hadoop!"
	exit 1
fi
if test "$status" != "R" 
then
	echo "Please first reserve nodes via pbsget in a separate window!!!"
	exit 1
fi

# check to see that the etc/hadoop/slaves  matches the nodes allocated to the user
cp etc/hadoop/slaves etc/hadoop/slaves.orig
qstat -1n | grep ${USER:0:8} | awk '{print $NF}' | awk -F+ '{for (i=1; i<=NF; i++) printf("%s\n", substr($i, 0, 6));}' > etc/hadoop/slaves

/bin/rm -fr logs pids
mkdir {logs,pids}

pdsh -w - < etc/hadoop/slaves  /bin/rm -fr /tmp/hadoop-`whoami`
pdsh -w - < etc/hadoop/slaves  mkdir /tmp/hadoop-`whoami`
pdsh -w - < etc/hadoop/slaves  chmod 700 /tmp/hadoop-`whoami`

cd etc/hadoop
for node in $(cat slaves)
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
