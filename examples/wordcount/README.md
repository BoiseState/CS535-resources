Wordcount
=========

Java version:  WordCount.java: Java solution for wordcount that uses a sequential approach with a dictionary
based on a HashMap.

```
java WordCount <input folder>
```

Python version: word_count.py --> Uses the same approach as the Java program.
```
./word_count.py <input folder>
```

wordcount.sh: A shell script solution for wordcount that uses a streaming approach.
```
./wordcount.sh <input folder>
```

Comparison of runtimes:

```
[amit@localhost Wordcount(master)]$ time java WordCount input > output

real    0m1.086s

[amit@localhost Wordcount(master)]$ time ./word_count.py input > output

real    0m0.712s

[amit@localhost Wordcount(master)]$ time wordcount.sh > output

real    0m3.332s
```

