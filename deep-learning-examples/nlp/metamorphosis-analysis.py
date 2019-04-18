# Databricks notebook source
# MAGIC %run /Users/mhuffer@allegient.com/credentials/azure-blob-connection

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

from pyspark.sql.functions import explode, col
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

from pyspark.sql.functions import lag, lead

explode_word_df = (filtered_words_df
                 .withColumn("filtered_word", explode((col("filtered"))))
                ).select("paragraph", "filtered_word")

explode_word_df.createOrReplaceTempView("explode_word_df")

ordered_word_df = spark.sql("""
                            SELECT
                              sub.*
                              , LAG(sub.filtered_word, 1, 0) OVER(PARTITION BY sub.paragraph ORDER BY sub.paragraph) AS prev_word
                              , LAG(sub.word_position, 1, 0) OVER(PARTITION BY sub.paragraph ORDER BY sub.paragraph) AS prev_word_pos
                              , LEAD(sub.filtered_word, 1, 0) OVER(PARTITION BY sub.paragraph ORDER BY sub.paragraph) AS next_word
                              , LEAD(sub.word_position, 1, 0) OVER(PARTITION BY sub.paragraph ORDER BY sub.paragraph) AS next_word_pos
                              , word_position - MIN(sub.word_position) OVER(PARTITION BY sub.paragraph ORDER BY sub.paragraph) AS distance_from_start_pos
                            FROM
                            (
                              SELECT 
                                *
                                , ROW_NUMBER() OVER(PARTITION BY paragraph ORDER BY paragraph) AS word_position 
                                FROM explode_word_df
                             ) AS sub
                            """)

display(ordered_word_df)

# COMMAND ----------

dbutils.widgets.text("word_search", "gregor")

# COMMAND ----------

word_to_search = dbutils.widgets.get("word_search")

try:
  display(ordered_word_df.select("filtered_word", "prev_word", "next_word", "paragraph", "distance_from_start_pos").where(col("filtered_word") == word_to_search).collect())
except:
  print(word_to_search + " could not be found in the document.  Please try another word!")

# COMMAND ----------

