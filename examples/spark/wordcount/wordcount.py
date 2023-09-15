#!/usr/bin/env python
# coding: utf-8

# The **wordcount** problem for multiple files using Spark in Python. This also shows how to interface with Hadoop HDFS.

# In[1]:


get_ipython().system('pip install findspark')


# In[2]:


get_ipython().system('pip install pyspark')


# In[3]:


import findspark


# In[4]:


import pyspark


# In[5]:


from pyspark.sql import SparkSession


# In[6]:


spark = SparkSession.builder.master('local[*]').appName("Tutorial-1").getOrCreate()


# In[7]:


sc = spark.sparkContext


# We only need the following five steps if we are running on Google colab. Uncomment these lines on Google colab.

# In[8]:


#from google.colab import files


# In[9]:


#uploaded = files.upload()


# In[10]:


#%mkdir input


# In[11]:


#%mv *.txt input


# In[12]:


#%ls


# In[13]:


#allFiles = sc.textFile('hdfs://localhost:9000/user/amit/input/*.txt')
allFiles = sc.textFile('input/*.txt')


# In[14]:


counts = allFiles.flatMap(lambda line:line.split(" ")).map(lambda word:(word,1)).reduceByKey(lambda x,y: x+y)


# In[15]:


#counts.saveAsTextFile("hdfs://localhost:9000/user/amit/output")


# Remove the results folder before saving new results.

# In[16]:


get_ipython().run_line_magic('rm', '-fr results')


# In[17]:


counts.saveAsTextFile("results") #saves as a folder


# We only take the first 10 as the whole list is very long.

# In[18]:


output = counts.take(10)


# In[19]:


for (word, count) in output:
    print('%s %i' % (word, count))

