# Databricks notebook source
import requests, json
import pandas as pd
from pandas.io.json import json_normalize
import datetime

# COMMAND ----------

r = requests.get('http://api.fantasy.nfl.com/v1/players/stats?statType=seasonStats&season=2010&week=1&format=json')

# COMMAND ----------

res = json.loads(r.text)
print(res)

# COMMAND ----------

pd_df = pd.DataFrame.from_dict(res, orient = 'columns')

# COMMAND ----------

df = sqlContext.createDataFrame(pd_df)
display(df)

# COMMAND ----------

res2 = res
pd_df2 = pd.DataFrame.from_dict(json_normalize(res2), orient = 'columns')

df2 = sqlContext.createDataFrame(pd_df2)
display(df2)

# COMMAND ----------

players = res["players"]

# COMMAND ----------

pd_df3 = pd.DataFrame.from_dict(json_normalize(players), orient = 'columns')

print(pd_df3)

# COMMAND ----------

from pyspark.sql.types import StructField, StringType, StructType, IntegerType, DoubleType

jsonSchema = StructType([ StructField("esbid", StringType(), True)
,StructField("gsisPlayerId", StringType(), True)
,StructField("id", StringType(), True)
,StructField("name", StringType(), True)
,StructField("position", StringType(), True)
,StructField("seasonProjectedPts", StringType(), True)
,StructField("seasonPts", StringType(), True)
,StructField("stats.1", StringType(), True)
,StructField("stats.2", StringType(), True)
,StructField("stats.3", StringType(), True)
,StructField("stats.4", StringType(), True)
,StructField("stats.5", StringType(), True)
,StructField("stats.6", StringType(), True)
,StructField("stats.7", StringType(), True)
,StructField("stats.8", StringType(), True)
,StructField("stats.9", StringType(), True)
,StructField("stats.10", StringType(), True)
,StructField("stats.11", StringType(), True)
,StructField("stats.12", StringType(), True)
,StructField("stats.13", StringType(), True)
,StructField("stats.14", StringType(), True)
,StructField("stats.15", StringType(), True)
,StructField("stats.16", StringType(), True)
,StructField("stats.17", StringType(), True)
,StructField("stats.18", StringType(), True)
,StructField("stats.20", StringType(), True)
,StructField("stats.21", StringType(), True)
,StructField("stats.22", StringType(), True)
,StructField("stats.23", StringType(), True)
,StructField("stats.24", StringType(), True)
,StructField("stats.25", StringType(), True)
,StructField("stats.26", StringType(), True)
,StructField("stats.27", StringType(), True)
,StructField("stats.28", StringType(), True)
,StructField("stats.30", StringType(), True)
,StructField("stats.31", StringType(), True)
,StructField("stats.32", StringType(), True)
,StructField("stats.33", StringType(), True)
,StructField("stats.34", StringType(), True)
,StructField("stats.35", StringType(), True)
,StructField("stats.36", StringType(), True)
,StructField("stats.37", StringType(), True)
,StructField("stats.38", StringType(), True)
,StructField("stats.39", StringType(), True)
,StructField("stats.41", StringType(), True)
,StructField("stats.42", StringType(), True)
,StructField("stats.43", StringType(), True)
,StructField("stats.44", StringType(), True)
,StructField("stats.45", StringType(), True)
,StructField("stats.46", StringType(), True)
,StructField("stats.47", StringType(), True)
,StructField("stats.48", StringType(), True)
,StructField("stats.49", StringType(), True)
,StructField("stats.50", StringType(), True)
,StructField("stats.51", StringType(), True)
,StructField("stats.52", StringType(), True)
,StructField("stats.53", StringType(), True)
,StructField("stats.54", StringType(), True)
,StructField("stats.55", StringType(), True)
,StructField("stats.56", StringType(), True)
,StructField("stats.57", StringType(), True)
,StructField("stats.58", StringType(), True)
,StructField("stats.59", StringType(), True)
,StructField("stats.60", StringType(), True)
,StructField("stats.61", StringType(), True)
,StructField("stats.62", StringType(), True)
,StructField("stats.63", StringType(), True)
,StructField("stats.64", StringType(), True)
,StructField("stats.65", StringType(), True)
,StructField("stats.66", StringType(), True)
,StructField("stats.67", StringType(), True)
,StructField("stats.68", StringType(), True)
,StructField("stats.69", StringType(), True)
,StructField("stats.70", StringType(), True)
,StructField("stats.71", StringType(), True)
,StructField("stats.72", StringType(), True)
,StructField("stats.73", StringType(), True)
,StructField("stats.74", StringType(), True)
,StructField("stats.75", StringType(), True)
,StructField("stats.76", StringType(), True)
,StructField("stats.77", StringType(), True)
,StructField("stats.78", StringType(), True)
,StructField("stats.79", StringType(), True)
,StructField("stats.81", StringType(), True)
,StructField("stats.82", StringType(), True)
,StructField("stats.83", StringType(), True)
,StructField("stats.84", StringType(), True)
,StructField("stats.85", StringType(), True)
,StructField("stats.86", StringType(), True)
,StructField("stats.87", StringType(), True)
,StructField("stats.88", StringType(), True)
,StructField("stats.89", StringType(), True)
,StructField("stats.90", StringType(), True)
,StructField("teamAbbr", StringType(), True)
,StructField("weekProjectedPts", StringType(), True)
,StructField("weekPts", StringType(), True) ])

# COMMAND ----------

df3 = sqlContext.createDataFrame(pd_df3, schema = jsonSchema)
display(df3)

# COMMAND ----------

basePath = '/nfl-data/fantasyJSON'
timeStampPath = '/' + str('{:%Y%m%d%H%M%S}'.format(datetime.datetime.now()))

print(basePath + timeStampPath)

df3.write.json(basePath + timeStampPath)

# COMMAND ----------

timeStampPath = '/' + str('{:%Y%m%d%H%M%S}'.format(datetime.datetime.now()))
df3.write.csv(basePath + timeStampPath)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /nfl-data/fantasyJSON/20181109010617/

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Convenience function for turning JSON strings into DataFrames
def jsonToDataFrame(json, schema = None):
  # Spark sessions are available with Spark 2.0+
  reader = spark.read
  if schema:
    reader.schema(schema)
  return reader.json(sc.parallelize([json]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Multiple API calls to generate large data frame with fantasy statistics

# COMMAND ----------

weeks = list(range(1,16))

response = []
frames = []

for week in weeks:
  # Create a dynamic dataframe name
  dfName = 'dfWeek_' + str(week)
  
  # Make request to REST api URI passing a single week as a parameter to the api call
  r = requests.get('http://api.fantasy.nfl.com/v1/players/stats?statType=seasonStats&season=2017&week='+str(week)+'&format=json')
  res = json.loads(r.text)
  
  # Write json response to file system for future analysis and long term retention
  timestamp = str('{:%Y%m%d%H%M%S}'.format(datetime.datetime.now()))

  with open('/dbfs/tmp/'+dfName+'_'+timestamp+'.json', 'w') as outfile:
    json.dump(dfName, outfile)
  
  # Create a dataframe that contains the dictionary of players from entire JSON object call
  playersDf = res["players"]
  
  # Create the pandas dataframe from dictionary for a given week
  dfName = pd.DataFrame.from_dict(json_normalize(playersDf), orient = 'columns')
  
  # Create a list that contains all of the JSON response objects
  response.append(json.loads(r.text))
  
  # Create a list that contains all of the panda dataframes for additional append
  frames.append(dfName)
  
  
    
#print(response)
print(frames)

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls /tmp

# COMMAND ----------

resultDF = pd.concat(frames)
print(resultDF)

# COMMAND ----------

resultDF.info(verbose = True)

# COMMAND ----------

from pyspark.sql.types import StructField, StringType, StructType, IntegerType, DoubleType

jsonSchema = StructType([ StructField("esbid", StringType(), True)
,StructField("gsisPlayerId", StringType(), True)
,StructField("id", StringType(), True)
,StructField("name", StringType(), True)
,StructField("position", StringType(), True)
,StructField("seasonProjectedPts", StringType(), True)
,StructField("seasonPts", StringType(), True)
,StructField("stats.1", StringType(), True)
,StructField("stats.2", StringType(), True)
,StructField("stats.3", StringType(), True)
,StructField("stats.4", StringType(), True)
,StructField("stats.5", StringType(), True)
,StructField("stats.6", StringType(), True)
,StructField("stats.7", StringType(), True)
,StructField("stats.8", StringType(), True)
,StructField("stats.9", StringType(), True)
,StructField("stats.10", StringType(), True)
,StructField("stats.11", StringType(), True)
,StructField("stats.12", StringType(), True)
,StructField("stats.13", StringType(), True)
,StructField("stats.14", StringType(), True)
,StructField("stats.15", StringType(), True)
,StructField("stats.16", StringType(), True)
,StructField("stats.17", StringType(), True)
,StructField("stats.18", StringType(), True)
,StructField("stats.20", StringType(), True)
,StructField("stats.21", StringType(), True)
,StructField("stats.22", StringType(), True)
,StructField("stats.23", StringType(), True)
,StructField("stats.24", StringType(), True)
,StructField("stats.25", StringType(), True)
,StructField("stats.26", StringType(), True)
,StructField("stats.27", StringType(), True)
,StructField("stats.28", StringType(), True)
,StructField("stats.29", StringType(), True)
,StructField("stats.30", StringType(), True)
,StructField("stats.31", StringType(), True)
,StructField("stats.32", StringType(), True)
,StructField("stats.33", StringType(), True)
,StructField("stats.34", StringType(), True)
,StructField("stats.35", StringType(), True)
,StructField("stats.36", StringType(), True)
,StructField("stats.37", StringType(), True)
,StructField("stats.38", StringType(), True)
,StructField("stats.39", StringType(), True)
,StructField("stats.41", StringType(), True)
,StructField("stats.42", StringType(), True)
,StructField("stats.43", StringType(), True)
,StructField("stats.44", StringType(), True)
,StructField("stats.45", StringType(), True)
,StructField("stats.46", StringType(), True)
,StructField("stats.47", StringType(), True)
,StructField("stats.48", StringType(), True)
,StructField("stats.49", StringType(), True)
,StructField("stats.50", StringType(), True)
,StructField("stats.51", StringType(), True)
,StructField("stats.52", StringType(), True)
,StructField("stats.53", StringType(), True)
,StructField("stats.54", StringType(), True)
,StructField("stats.55", StringType(), True)
,StructField("stats.56", StringType(), True)
,StructField("stats.57", StringType(), True)
,StructField("stats.58", StringType(), True)
,StructField("stats.59", StringType(), True)
,StructField("stats.60", StringType(), True)
,StructField("stats.61", StringType(), True)
,StructField("stats.62", StringType(), True)
,StructField("stats.63", StringType(), True)
,StructField("stats.64", StringType(), True)
,StructField("stats.65", StringType(), True)
,StructField("stats.66", StringType(), True)
,StructField("stats.67", StringType(), True)
,StructField("stats.68", StringType(), True)
,StructField("stats.69", StringType(), True)
,StructField("stats.70", StringType(), True)
,StructField("stats.71", StringType(), True)
,StructField("stats.72", StringType(), True)
,StructField("stats.73", StringType(), True)
,StructField("stats.74", StringType(), True)
,StructField("stats.75", StringType(), True)
,StructField("stats.76", StringType(), True)
,StructField("stats.77", StringType(), True)
,StructField("stats.78", StringType(), True)
,StructField("stats.79", StringType(), True)
,StructField("stats.81", StringType(), True)
,StructField("stats.82", StringType(), True)
,StructField("stats.83", StringType(), True)
,StructField("stats.84", StringType(), True)
,StructField("stats.85", StringType(), True)
,StructField("stats.86", StringType(), True)
,StructField("stats.87", StringType(), True)
,StructField("stats.88", StringType(), True)
,StructField("stats.89", StringType(), True)
,StructField("stats.90", StringType(), True)
,StructField("stats.91", StringType(), True)
,StructField("stats.92", StringType(), True)
,StructField("stats.93", StringType(), True)
,StructField("teamAbbr", StringType(), True)
,StructField("weekProjectedPts", StringType(), True)
,StructField("weekPts", StringType(), True) ])

# COMMAND ----------

jsonSchema

# COMMAND ----------

df = sqlContext.createDataFrame(resultDF, schema = jsonSchema)
display(df)

# COMMAND ----------

df.registerTempTable("test")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   test
# MAGIC WHERE name = "Danny Amendola"