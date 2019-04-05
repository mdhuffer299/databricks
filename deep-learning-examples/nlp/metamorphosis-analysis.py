# Databricks notebook source
spark.conf.set("fs.azure.account.key.mhufferblob.blob.core.windows.net","replace with key")

# COMMAND ----------

dbutils.fs.ls("wasbs://text-documents@mhufferblob.blob.core.windows.net/")

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id
from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer, StringIndexer, RegexTokenizer, StopWordsRemover
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

text_df = (spark
           .read
           .format("text")
           .load("wasbs://text-documents@mhufferblob.blob.core.windows.net/metamorphosis_clean.txt")
          ).withColumn("paragraph", monotonically_increasing_id())

display(text_df)

# COMMAND ----------

text_df.count()

# COMMAND ----------

regex_tokenizer = RegexTokenizer(inputCol = "value", outputCol = "words", pattern = "\\W")
raw_words = regex_tokenizer.transform(text_df)

display(raw_words)

# COMMAND ----------

remover = StopWordsRemover(inputCol = "words", outputCol = "filtered")

words_df = remover.transform(raw_words)

display(words_df.select("filtered", "paragraph"))

# COMMAND ----------

from pyspark.sql.functions import explode
from pyspark.sql.types import IntegerType, FloatType, DoubleType

filtered_words_df = words_df.select("filtered", "paragraph")

word_count_df = (filtered_words_df
                 .withColumn("filtered_word", explode((col("filtered"))))
                 .groupBy("filtered_word")
                 .count()
                 .sort("count", ascending = False)
                ).withColumn("string_index", monotonically_increasing_id())

#word_count_df = word_count_df.withColumn("string_index", word_count_df["filtered_word"].cast(DoubleType()))

display(word_count_df)

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

# COMMAND ----------

