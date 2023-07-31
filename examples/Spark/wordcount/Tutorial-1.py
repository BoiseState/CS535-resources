#!/usr/bin/env python
# coding: utf-8

# In[3]:


import findspark


# In[4]:


import pyspark


# In[5]:


from pyspark.sql import SparkSession


# In[16]:


spark = SparkSession.builder.master('local[*]').appName("Tutorial-1").getOrCreate()


# In[17]:


sc = spark.sparkContext


# In[30]:


textfile = sc.textFile('test-data.txt')


# In[59]:


counts = textfile.flatMap(lambda line:line.split(" ")).map(lambda word:(word,1)).reduceByKey(lambda x,y: x+y)


# In[60]:


output = counts.collect()


# In[61]:


for (word, count) in output:
    print('%s %i' % (word, count))


# In[62]:


spark.stop()


# In[ ]:




