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
nltk.download("stopwords")

# COMMAND ----------

from nltk.corpus import stopwords

stopwords_list = set(stopwords.words("english"))

print(stopwords_list)

# COMMAND ----------

