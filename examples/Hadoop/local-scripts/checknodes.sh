$#!/bin/bash

for f in $(workers)
do
	echo "=== Checking node $host for already running Hadoop ==="
	timeout -1 ssh ps augx | grep hadoop
done
