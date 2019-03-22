# Databricks notebook source
dbutils.widgets.text("dataframe", "No Dataframe Defined")
dbutils.widgets.text("outputPath", "No Path Defined")
dbutils.widgets.dropdown("outputFileType", "csv", ["csv", "json", "parquet", "orc"])

# COMMAND ----------

#dbutils.widgets.removeAll()

# COMMAND ----------

inputDF = dbutils.widgets.get("dataframe")
outputPath = dbutils.widgets.get("outputPath")
outputFileType = dbutils.widgets.get("outputFileType")

# COMMAND ----------

global_temp_db = spark.conf.get("spark.sql.globalTempDatabase")
df = table(global_temp_db + "." + inputDF)

# COMMAND ----------

try:
  dirExists = dbutils.fs.ls(outputPath)
except:
  dirExists = False

if dirExists == False:
  dbutils.fs.mkdirs(outputPath)
  print(outputPath + " has been created")
else:
  print("Output directory already exists!")

# COMMAND ----------

(df
 .write
 .format(outputFileType)
 .mode("overwrite")
 .save(outputPath)
)

# COMMAND ----------

dbutils.notebook.exit("Notebook Completed: Data was loaded to " + outputPath)

# COMMAND ----------

#dbutils.fs.rm("/tmp/" + outputFileType, True)