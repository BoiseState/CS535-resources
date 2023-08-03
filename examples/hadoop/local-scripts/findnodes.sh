#!/bin/bash
# Finds n machines that are up for use in a Hadoop cluster.
# It skips the master node, which is the current machine
# author: amit

case $# in 
0) echo "Usage $0 <#nodes>"; exit 1;;
esac


request=$1
num=0
#generate random startnode at 10 or higher
startnode=$[10 + 8 *(RANDOM % 10)]
next=$startnode
echo -n > workers #create new empty file
while [ $num -lt  $request ]
do 
	node=$(printf %s%02d  "onyxnode" $next)

	timeout 1 ssh -o stricthostkeychecking=no $node  date >& /dev/null
	if test "$?" = "0" 
	then
		if test "$node" != "$(hostname -s)"
		then
			num=$[num+1]
			echo "Found $num good nodes so far: $node"
			echo "$node" >> workers
		else
			echo "skipping master node: " + $node
		fi
		if test "$num" = "$request"
		then
			echo "Done. Check node names in file named workers"
			echo
			break
		fi
	fi 
	next=$[next+1]
	if test "$next" = "100"
	then
		next=1
	fi
done


echo
echo "Now testing for if Hadoop is running on these nodes"
echo

./checknodes.sh
