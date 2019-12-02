#!/bin/bash

case $# in 
0) echo "Usage $0 <#nodes>"; exit 1;;
esac


request=$1
num=0
#generate random startnode at 10 or higher
startnode=$[10 + 8 *(RANDOM % 10)]
echo -n > workers #create new empty file
for i in {1..100}
do 
	node=onyxnode$[startnode+i]
	timeout 1 ssh -o stricthostkeychecking=no $node  date >& /dev/null
	if test "$?" = "0" 
	then
		echo "$node" >> workers
		num=$[num+1]
		echo "Found $num good nodes so far: $node"
		if test "$num" = "$request"
		then
			echo "Done. Check node names in file named workers"
			exit
		fi
	fi 
done

