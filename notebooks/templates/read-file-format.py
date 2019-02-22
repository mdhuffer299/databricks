# Databricks notebook source
dbutils.widgets.text("fileName", "No File Provided")
dbutils.widgets.text("inputPath", "No Path Provided")
dbutils.widgets.dropdown("inputFileType", "csv", ["csv", "json", "parquet", "orc"])

# COMMAND ----------

#/adult.data
#/databricks-datasets/adult
#csv
#dbutils.widgets.removeAll()

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

df.createOrReplaceGlobalTempView("outputDF")

# COMMAND ----------

dbutils.notebook.exit("outputDF")

# COMMAND ----------

#dbutils.fs.rm("/tmp/parquet", True)

# COMMAND ----------

