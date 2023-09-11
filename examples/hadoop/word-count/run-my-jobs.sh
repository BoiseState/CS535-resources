#!/bin/bash

for job in 1 2 3 4
do
	hadoop jar wc.jar WordCount input output$job >& job$job.log &
	#sleep 3600
done

