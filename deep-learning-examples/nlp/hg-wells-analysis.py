# Databricks notebook source
# MAGIC %run /Users/mhuffer@allegient.com/credentials/azure-blob-connection

# COMMAND ----------

dbutils.fs.ls("abfss://nlp-data@hufferadls.dfs.core.windows.net/hg-wells")

# COMMAND ----------

basePath = "abfss://nlp-data@hufferadls.dfs.core.windows.net/hg-wells"

# COMMAND ----------

new_machiavelli = "/new-machiavelli.txt"

df = (spark
      .read
      .format("text")
      .load(basePath + new_machiavelli)
     )

display(df)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

display(df.replace("||", " "))

# COMMAND ----------

from pyspark.sql.functions import col, UserDefinedFunction
from pyspark.sql.types import StringType

udfReplaceValues = UserDefinedFunction(lambda x: x.replace("||", " "), StringType())

new_df = (df.withColumn("new_column", udfReplaceValues(col("value")))).select("new_column")

display(new_df)

# COMMAND ----------

import nltk
nltk.download("stopwords", "punkt")

# COMMAND ----------

from nltk.corpus import stopwords

stopwords_list = set(stopwords.words("english"))

print(stopwords_list)

# COMMAND ----------

from pyspark.ml.feature import Tokenizer

tokenizer = Tokenizer(inputCol="new_column", outputCol="token_column")

token_df = tokenizer.transform(new_df).select("token_column")

display(token_df)

# COMMAND ----------

from pyspark.ml.feature import StopWordsRemover

clean_token = StopWordsRemover(inputCol="token_column", outputCol="clean_token_column")
clean_token_df = clean_token.transform(token_df).select("clean_token_column")

display(clean_token_df)

# COMMAND ----------

from pyspark.ml.feature import Word2Vec

# Learn a mapping from words to Vectors.
word2Vec = Word2Vec(vectorSize=3, minCount=0, inputCol="clean_token_column", outputCol="result")
model = word2Vec.fit(clean_token_df)

result = model.transform(clean_token_df)
for row in result.collect():
    text, vector = row
    print("Text: [%s] => \nVector: %s\n" % (", ".join(text), str(vector)))

# COMMAND ----------

print(result.collect())

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np



word = np.array(word_count_df.select("filtered_word").take(25))
word_index = np.array(word_count_df.select("string_index").take(25))
word_count = np.array(word_count_df.select("count").take(25))

fig, ax = plt.subplots()
ax.plot(word_index, word_count, 'ro')
ax.set_xticks(word_index)
ax.set_xticklabels(labels = word, rotation = 55, size = 8)
ax.plot(word_index, word_count, 'k-')

display(fig)