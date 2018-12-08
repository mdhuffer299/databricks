# Databricks notebook source
import glob
import pandas as pd

# COMMAND ----------

path = '/dbfs/mnt/hospital-ratings/*.csv'
files = glob.glob(path)

for file in files:
  fileIndex = files.index(file)
  files[fileIndex] = files[fileIndex].replace("/dbfs","")
  df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(files[fileIndex])
  dfName = files[fileIndex].replace("/mnt/hospital-ratings/","").replace(".csv","").replace(" ","_").replace("-","")
  items = df.columns
  for item in items:
    listIndex = items.index(item)
    items[listIndex] = items[listIndex].replace(" ","_").replace("-","").replace(",","").replace("(","").replace(")","").replace("{","").replace("}","")

  newDF = df.toDF(*items)
 
  #print(items)
  #print(df.columns)
  newDF.write.mode("overwrite").saveAsTable("hospital_stg."+str(dfName))
  