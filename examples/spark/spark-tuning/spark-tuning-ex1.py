#!/usr/bin/env python
# coding: utf-8

# In[25]:


from pyspark import SparkContext, SparkConf


# In[26]:


from pyspark.sql import SparkSession


# In[27]:


spark = SparkSession.builder.master('local[*]').appName("spark-tuning").getOrCreate()


# In[28]:


sc = spark.sparkContext


# In[29]:


spark.conf.set("spark.executor.memory", "4g")


# In[16]:


textData = sc.textFile("../word-count/input/*.txt")


# In[18]:


parquetData = textData.toDF("STRING").write.parquet("data.parquet")


# In[19]:


parquetData2 = spark.read.parquet("data.parquet")


# In[20]:


parquetData2.count()


# In[21]:


textData.count()


# In[24]:


sc.stop()


# In[ ]:




