
# coding: utf-8

# In[1]:


words = sc.parallelize (
   ["scala", 
   "java", 
   "hadoop", 
   "spark", 
   "akka",
   "spark vs hadoop", 
   "pyspark",
   "pyspark and spark"]
)


# In[10]:


words_filter = words.filter(lambda x: 'spark' in x)


# In[11]:


words_filter.collect()
    


# In[12]:


words_map = words.map(lambda x: (x, 1))


# In[13]:


mapping = words_map.collect()


# In[14]:


mapping


# In[15]:


from operator import add


# In[16]:


reducing = words_map.reduceByKey(add)


# In[17]:


reducing.collect()

