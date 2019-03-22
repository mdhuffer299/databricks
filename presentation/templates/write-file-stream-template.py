# Databricks notebook source
dbutils.widgets.text("inputStreamDataframe", "No Streaming Dataframe Defined")
dbutils.widgets.text("writeStreamPath", "No Write Stream Path Defined")
dbutils.widgets.text("writeStreamCheckpointPath", "No Write Stream Checkpoint Location Defined")
dbutils.widgets.dropdown("writeStreamFileType", "csv", ["csv", "json", "parquet", "orc", "delta"])
dbutils.widgets.dropdown("writeStreamOutputMode", "complete", ["complete", "append", "update"])

# COMMAND ----------

readStreamDF = dbutils.widgets.get("inputStreamDataFrame")
writeStreamPath = dbutils.widgets.get("writeStreamPath")
writeStreamCheckpointPath = dbutils.widgets.get("writeStreamCheckpointPath")
writeStreamFileType = dbutils.widgets.get("writeStreamFileType")
writeStreamOutputMode = dbutils.widgets.get("writeStreamOutputMode")

# COMMAND ----------

(readStreamDF                                                            # Get the dataframe passed in from notebook workflow
  .writeStream                                                           # Write the stream
  .format(writeStreamFileType)                                           # Use the delta format
  .option("checkpointLocation", writeStreamCheckpointPath)               # Specify where to log metadata
  .option("path", writeStreamPath)                                       # Specify the output path
  .outputMode(writeStreamOutputMode)                                     # Append new records to the output path
  .start()                                                               # Start the operation
)