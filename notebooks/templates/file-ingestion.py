# Databricks notebook source
dbutils.widgets.text("fileName", "no file")
dbutils.widgets.text("inputDir", "no directory")
dbutils.widgets.text("outputDir", "no directory")
dbutils.widgets.dropdown("inputFileType", "csv", ["csv", "json", "parquet"])
dbutils.widgets.dropdown("outputFileType", "csv", ["csv", "json", "parquet"])

# COMMAND ----------

#dbutils.widgets.removeAll()

# COMMAND ----------

fileName = dbutils.widgets.get("fileName")
inputDir = dbutils.widgets.get("inputDir")
outputDir = dbutils.widgets.get("outputDir")
inputFileType = dbutils.widgets.get("inputFileType")
outputFileType = dbutils.widgets.get("outputFileType")

# COMMAND ----------

df = (
  spark
  .read
  .format(inputFileType)
  .option("inferSchema", "True")
  .load(inputDir + fileName)
)

# COMMAND ----------

try:
  dirExists = dbutils.fs.ls(outputDir)
except:
  dirExists = False

if dirExists == False:
  dbutils.fs.mkdirs(outputDir)
  print(outputDir + " has been created")
else:
  print("Output directory already exists!")

# COMMAND ----------

(df
 .write
 .format(outputFileType)
 .mode("overwrite")
 .save(outputDir)
)

# COMMAND ----------

dbutils.notebook.exit("Notebook Completed: Data was loaded to " + outputDir)

# COMMAND ----------

#dbutils.fs.rm("/tmp/parquet", True)

# COMMAND ----------

