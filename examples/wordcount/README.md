Wordcount
=========

WordCount.java: Java solution for wordcount that uses a sequential approach with a dictionary
based on a HashMap.

wordcount.sh: A shell script solution for wordcount that uses a streaming approach.

Comparison of runtimes:

```
[amit@localhost Wordcount(master)]$ time java WordCount input/ >& log

real    0m4.218s
user    0m4.552s
sys     0m0.995s
[amit@localhost Wordcount(master)]$ time wordcount.sh >& log

real    0m7.262s
user    0m7.732s
sys     0m0.200s
```

