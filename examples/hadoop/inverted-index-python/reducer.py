#!/usr/bin/python

from sys import stdin
import re
 
current_word = None
word = None
index = ''

for line in stdin:
    line = line.strip()
    word, postings = line.split('\t', 1)

    if current_word == word:
        index = postings + ',' + index
    else:
        if current_word:
            print(current_word, index)
        current_word = word
        index = postings

if current_word == word:
    print(current_word, index)

