#!/usr/bin/python
"""mapper.py"""

from sys import stdin
import sys
import re
import os

stdin = open(sys.stdin.fileno(), encoding='iso-8859-1', mode='r')

for line in stdin:
        doc_id = os.environ["map_input_file"]
        # try to match only alphabetic words
        words = re.findall(r'\b[A-Za-z]+\b', line.strip())
        doc_name = os.path.split(doc_id)[-1]

        # Map the words
        for word in words:
                print("%s\t%s" % (word.lower(), doc_name))
