#!/bin/bash

pdsh -w onyxnode[01-99] ps augx | grep hadoop  | awk '{print $1, " ", $3}' | awk -F: '{print $1, " ", $2}' > list.$$


cat list.$$ | while read f
do
	host=$(echo $f | awk '{print $1}')
	pid=$(echo $f | awk '{print $2}')
	echo "...attempting to kill $f"
	ssh -n  $host sudo kill -9 $pid
done 

/bin/rm -f list.$$
