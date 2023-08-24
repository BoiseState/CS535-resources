#!/bin/bash

case $# in
0) echo "Usage: ./wordcount.sh <input folder>"; exit 1;;
esac

cat $1/* | tr ' ' '\n' | sort | uniq -c
