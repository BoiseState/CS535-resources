#!/bin/bash

echo "=== checknodes.sh: workers file from the folder $(pwd) ==="
echo

for host in $(cat workers)
do
	echo "=== Checking node $host for already running Hadoop ==="
	timeout 1 ssh -o stricthostkeychecking=no $host "ps augx | grep java | grep hadoop | grep -v grep"
done
	echo
	echo "Remove nodes from workers file that had hadoop already running on it!"
	echo
