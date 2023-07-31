#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark


# In[2]:


import pyspark


# In[3]:


from pyspark.sql import SparkSession


# In[4]:


spark = SparkSession.builder.master('local[*]').appName("Tutorial-1").getOrCreate()


# In[5]:


sc = spark.sparkContext


# In[12]:


allFiles = sc.textFile('input/*.txt')


# In[13]:


counts = allFiles.flatMap(lambda line:line.split(" ")).map(lambda word:(word,1)).reduceByKey(lambda x,y: x+y)


# In[17]:


counts.saveAsTextFile("hdfs://localhost:9000/user/amit/output")


# In[16]:


counts.saveAsTextFile("results.txt")


# In[14]:


output = counts.collect()


# In[15]:


for (word, count) in output:
    print('%s %i' % (word, count))


# In[ ]:




