
# coding: utf-8

# In[2]:


from pyspark.context import SparkContext

from pyspark.sql import HiveContext

sqlContext = HiveContext(sc)


data = sqlContext.sql("select * from metrixa_global_database.global_summary_partitioned_manulaly limit 10")


# In[3]:


data.show()

