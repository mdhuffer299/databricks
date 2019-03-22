# Databricks notebook source
dbutils.widgets.text("readStreamPath", "No Stream Path Defined")
dbutils.widgets.text("readStreamFileName", "No Stream File Name Defined")
dbutils.widgets.dropdown("readStreamFileType", "csv", ["csv", "json", "parquet", "orc", "delta"])
dbutils.widgets.text("readStreamCheckpointLocation", "No Stream Checkpoint Location Defined")

# COMMAND ----------

readStreamPath = dbutils.widgets.get("readStreamPath")
readStreamFileName = dbutils.widgets.get("readStreamFileName")
readStreamFileType = dbutils.widgets.get("readStreamFileType")
readStreamCheckpointLocation = dbutils.widgets.get("readStreamCheckpointLocation")

# COMMAND ----------

streamDF = (spark
            .readStream
            .format(readStreamFileType)
            .option("checkpointLocation", readStreamCheckpointLocation)
            .option("path", readStreamPath + readStreamFileName)
            .load()
           )

# COMMAND ----------

streamDF.createOrReplaceGlobalTempView("readStreamFileOutputDF")

# COMMAND ----------

dbutils.notebook.exit("readStreamFileOutputDF")