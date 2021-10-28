#!/bin/bash

echo "=== checknodes.sh: workers file from the folder $(pwd) ==="
echo

echo > busy-workers
echo > checknodes.log

for host in $(cat workers)
do
	echo -n "=== Checking node $host ==="
	echo $host >> checknodes.log
	timeout 1 ssh -o stricthostkeychecking=no $host "ps augx | grep java | grep hadoop | grep -v grep" | grep hadoop >> checknodes.log
	if test "$?" = "0"
	then
		echo " already has Hadoop running on it"
		echo $host >> busy-workers
	else
		echo " is available"
	fi

done
	echo
	echo "Busy nodes listed in the file busy-workers. "
	echo "See checknodes.log for details of who has Hadoop running on the nodes."
	echo
