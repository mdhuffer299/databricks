# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Setup
# MAGIC In order to run the following notebook, you must install the deep learning libraries listed below:
# MAGIC * A Maven library with coordinate `1.4.0-spark2.4-s_2.11` or higher
# MAGIC * PyPI libraries with packages `tensorflow==1.12.0`, `keras==2.2.4`, `h5py==2.7.0`, and `wrapt`

# COMMAND ----------

dbutils.fs.mkdirs('/images/')
dbutils.fs.mkdirs("/images/full_image_df")

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC wget -c http://www.vision.caltech.edu/Image_Datasets/Caltech101/101_ObjectCategories.tar.gz -O - | tar -xz -C /tmp/images/

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls file:/tmp/images/101_ObjectCategories/

# COMMAND ----------

  dbutils.fs.mv("file:/tmp/images/101_ObjectCategories/", "/images/", True)

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls /images/

# COMMAND ----------

from pyspark.ml.image import ImageSchema
from pyspark.sql.functions import lit
import os, sys

# Open a file
path = '/dbfs/images/'
dirs = os.listdir( path )

# This would print all the files and directories
directory_list = []

for file in dirs:
   directory_list.append(file)
    
directory_list.pop(0)

base_image_dir = '/images/'

full_image_df = 0

for dir in directory_list:
  full_image_dir = base_image_dir + "/" + dir
  if full_image_df == 0:
    df = ImageSchema.readImages(full_image_dir)
    df = df.withColumn("image_label", lit(dir.lower()))
    full_image_df = df
  else:
    df = ImageSchema.readImages(full_image_dir)
    df = df.withColumn("image_label", lit(dir.lower()))
    full_image_df = full_image_df.union(df)

full_image_df.write.format("parquet").mode("overwrite").save("/images/full_image_df/")

# COMMAND ----------

display(full_image_df.where)

# COMMAND ----------

idf = (spark
       .read
       .format("parquet")
       .load("/images/full_image_df")
      )

display(idf)

# COMMAND ----------

train_df, test_df = idf.randomSplit([0.6, 0.4])

# train logistic regression on features generated by InceptionV3:
from sparkdl import DeepImageFeaturizer
featurizer = DeepImageFeaturizer(inputCol="image", outputCol="features", modelName="InceptionV3")

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer

# Index string label column
stringIndexer = StringIndexer(inputCol = "image_label", outputCol = "label")

# Build our logistic regression transformation
lr = LogisticRegression(maxIter=20, regParam=0.05, elasticNetParam=0.3, labelCol="label")

# Build our ML piepline
p = Pipeline(stages=[featurizer, stringIndexer, lr])

# Build our model
p_model = p.fit(train_df)

# Run our model against the test dataset
tested_df = p_model.transform(test_df)

# Evaluate our model
evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
print("Test set accuracy = " + str(evaluator.evaluate(tested_df.select("prediction", "label"))))

# COMMAND ----------

display(tested_df)