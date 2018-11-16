# Databricks notebook source
import datetime
import pandas as pd
from pyspark.sql.functions import lit

# COMMAND ----------

dbutils.fs.mkdirs("/mnt/nfl-data/pbp-get")

# COMMAND ----------

# MAGIC %sh wget
# MAGIC 
# MAGIC for i in {2009..2018}
# MAGIC do
# MAGIC   wget https://raw.githubusercontent.com/ryurko/nflscrapR-data/master/play_by_play_data/regular_season/reg_pbp_"$i".csv -O reg_pbp_"$i".csv -O /tmp/reg-pbp-"$i".csv
# MAGIC done

# COMMAND ----------

years = list(range(2009,2019))

timestamp = str('{:%Y-%m-%d}'.format(datetime.datetime.now()))

for year in years:
  dbutils.fs.mv("file:///tmp/reg-pbp-"+str(year)+".csv", "/mnt/nfl-data/pbp-get/"+timestamp+"/pbp-"+str(year)+".csv")

# COMMAND ----------

years = list(range(2009,2019))

timestamp = str('{:%Y-%m-%d}'.format(datetime.datetime.now()))
basePath = "/mnt/nfl-data/pbp-get/"+timestamp+"/pbp-"

for year in years:
  dataPath = basePath+str(year)+".csv"
  nflDF = spark.read.format("csv").option("header","true").option("inferSchema","true").load(dataPath)
  
  nflDF = nflDF.withColumn("season", lit(year))
  
  nflDF.write.format("csv").option("header","true").save("/mnt/nfl-data/pbp-get/test/pbp-"+str(year))

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls /mnt/nfl-data/pbp-get/test/pbp-2009

# COMMAND ----------

dbutils.fs.rm('/mnt/nfl-data/pbp-get/test',True)

# COMMAND ----------

