
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession


# In[2]:


spark = SparkSession     .builder     .appName("Python Spark SQL basic example")     .getOrCreate()


# In[4]:


df = spark.read.csv("hdfs://hdp01.metrixa.local:8020/phong/test_df.csv",header=True);


# In[6]:


print type(df)


# In[8]:


df.show()


# In[9]:


df.select("MTXTRACK_ID").show()


# In[10]:


df.createOrReplaceTempView("temp_conversion")


# In[15]:


spark.sql("select * from (select CustomerID, sum(Quantity) as quantity from temp_conversion group by CustomerID) a order by a.quantity DESC limit 10").show()


# In[28]:


from pyspark.sql.functions import lit
from pyspark.sql.functions import udf
def custom_field(CustomerID):
    return CustomerID
df = df.withColumn("test",udf(custom_field)(df.CustomerID))


# In[20]:


spark.sql("select * from temp_conversion limit 1")


# In[29]:


df.select("test").show()


# In[ ]:


d

