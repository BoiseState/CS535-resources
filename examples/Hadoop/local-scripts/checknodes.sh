#!/bin/bash

for host in $(cat workers)
do
	echo "=== Checking node $host for already running Hadoop ==="
	timeout 1 ssh -o stricthostkeychecking=no $host "ps augx | grep java | grep hadoop | grep -v grep"
done
