# Databricks notebook source
dbutils.widgets.text("fileName", "No File Provided")
dbutils.widgets.text("inputPath", "No Path Provided")
dbutils.widgets.dropdown("inputFileType", "csv", ["csv", "json", "parquet", "orc"])

# COMMAND ----------

fileName = dbutils.widgets.get("fileName")
inputPath = dbutils.widgets.get("inputPath")
inputFileType = dbutils.widgets.get("inputFileType")

# COMMAND ----------

df = (
  spark
  .read
  .format(inputFileType)
  .option("inferSchema", "True")
  .load(inputPath + fileName)
)

# COMMAND ----------

df.createOrReplaceGlobalTempView("readFileOutputDF")

# COMMAND ----------

dbutils.notebook.exit("readFileOutputDF")