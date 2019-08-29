#!/bin/bash

cat input/* | tr ' ' '\n' | sort | uniq -c
