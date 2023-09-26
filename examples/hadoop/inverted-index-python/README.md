
# Building and running Hadoop MapReduce jobs using streaming Python

## To test directly, set the environment variable

```
export map_input_file="test.txt"
```

First test the mapper:

```
cat test.text | ./mapper.py 
```
Then test the mapper and reducer together:

```
cat test.txt | ./mapper.py | sort | ./reducer.py
```

and debug any basic Python issues first!

## To run:

Make sure you have HDFS running either in standalone or pseudo-distributed mode before doing
the following steps. 

```
hdfs  dfs -put ../input
 
hadoop jar ~/hadoop-install/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar -mapper mapper.py -reducer reducer.py -input input -output output -file ./mapper.py -file ./reducer.py

hdfs dfs -get output
```


# Sample output

See below for a snippet from the output file (that is very large so we are showing a few sample
lines).

```
...
adversary	Les-Miserables.txt, Les-Miserables.txt, Les-Miserables.txt,
        Complete-Shakespeare.txt, Complete-Shakespeare.txt, Complete-Shakespeare.txt,
        Complete-Shakespeare.txt, Complete-Shakespeare.txt, Complete-Shakespeare.txt,
        Complete-Shakespeare.txt, Complete-Shakespeare.txt, Complete-Shakespeare.txt, Patrick-Henry.txt,
        Encyclopaedia.txt, Encyclopaedia.txt, Encyclopaedia.txt, Encyclopaedia.txt, Encyclopaedia.txt,
        Encyclopaedia.txt

...
critically	Gift-of-the-Magi.txt, Encyclopaedia.txt
...
wonderland	Through-the-Looking-Glass.txt, Alice-in-Wonderland.txt, Alice-in-Wonderland.txt,
        Alice-in-Wonderland.txt, Alice-in-Wonderland.txt

...

yell	Complete-Shakespeare.txt, Complete-Shakespeare.txt, Complete-Shakespeare.txt,
        Tom-Sawyer-Abroad.txt, Tom-Sawyer-Abroad.txt, Tom-Sawyer-Abroad.txt, Tom-Sawyer-Abroad.txt,
        Les-Miserables.txt

...
```

