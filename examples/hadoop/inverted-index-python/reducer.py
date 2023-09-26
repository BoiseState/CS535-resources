#!/usr/bin/python

from sys import stdin
import re
 
current_word = None
word = None
doclist = ''

for line in stdin:
    #remove leading and trailing whitespace (including newlines)
    line = line.strip()
    #split into word and document name
    word, document = line.split('\t', 1)

    #while the word is the same, keep appending to the document list
    if current_word == word:
        doclist = document + ', ' + doclist
    else:
    #otherwise we received a new word, so print the existing list and start a new one
        if current_word:
            print(current_word, doclist)
        current_word = word
        doclist = document

# print the last list
if current_word == word:
    print(current_word, doclist)

