# Project #: Project Name

* Author: Your Name
* Class: CS535 Section 1
* Semester: 

## Overview

Concisely explain what the program does. If this exceeds a couple of
sentences, you're going too far. Generally you should be pulling this
right from the project specification. I don't want you to just cut and
paste, but paraphrase what is stated in the project specification.

## Reflection

Write a two paragraph reflection describing your experience with this 
project.  Talk about what worked well and what was challenging.  
Did you run into an issue that took some time to figure out?  
Tell us about it. What did you enjoy, what was less desirable? Feel
free to add other items (within the two paragraph limit).

## Compiling and Using
### Prerequisets
You must have hadoop and Java8 installed on your system

### Running the Code
importing input files
```
hdfs dfs -put <input files> input
```
running hadoop
```
hadoop jar inverted-index.jar input output
```
getting results
```
hdfs dfs -get output <output location>
```
## Results 
Local Machine :  **0m11sec**

cscluster00 :    **6m21s**

## Sources used

The only bug that I used outside sources to solve was a the error "Compararator Violated the General Contract" and "Out of Memory Error"

To solve this I had to adjusted my comparator to using Intiger.compare() and changed to legacy mergeSort. Sources are found below.
https://stackoverflow.com/questions/11441666/java-error-comparison-method-violates-its-general-contract
https://www.baeldung.com/java-comparator-comparable
