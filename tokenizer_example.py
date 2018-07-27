
# coding: utf-8

# In[1]:


from __future__ import print_function

# $example on$
from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType
# $example off$
from pyspark.sql import SparkSession


# In[2]:


spark = SparkSession        .builder        .appName("TokenizerExample")        .getOrCreate()


# In[3]:


# $example on$
    sentenceDataFrame = spark.createDataFrame([
        (0, "Hi I heard about Spark"),
        (1, "I wish Java could use case classes"),
        (2, "Logistic,regression,models,are,neat")
    ], ["id", "sentence"])


# In[4]:


# $example on$
sentenceDataFrame = spark.createDataFrame([
        (0, "Hi I heard about Spark"),
        (1, "I wish Java could use case classes"),
        (2, "Logistic,regression,models,are,neat")
    ], ["id", "sentence"])


# In[5]:


sentenceDataFrame


# In[6]:


tokenizer = Tokenizer(inputCol="sentence", outputCol="words")


# In[7]:


regexTokenizer = RegexTokenizer(inputCol="sentence", outputCol="words", pattern="\\W")


# In[8]:


countTokens = udf(lambda words: len(words), IntegerType())


# In[9]:


print type(countTokens)


# In[10]:


print type(countTokens)


# In[11]:


countTokens


# In[12]:


tokenized = tokenizer.transform(sentenceDataFrame)


# In[13]:


tokenized.select("sentence", "words")        .withColumn("tokens", countTokens(col("words"))).show(truncate=False)


# In[14]:


regexTokenized = regexTokenizer.transform(sentenceDataFrame)


# In[15]:


regexTokenized.select("sentence", "words")         .withColumn("tokens", countTokens(col("words"))).show(truncate=False)

