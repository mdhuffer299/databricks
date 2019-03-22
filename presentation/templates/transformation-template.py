# Databricks notebook source
dbutils.widgets.text("dataframe", "No Dataframe Defined")

# COMMAND ----------

inputDF = dbutils.widgets.get("dataframe")

# COMMAND ----------

global_temp_db = spark.conf.get("spark.sql.globalTempDatabase")
df = table(global_temp_db + "." + inputDF)

# COMMAND ----------

transformedDF = (df.
                 # Insert the transformations you would like to perform
                )

# COMMAND ----------

transformedDF.createOrReplaceGlobalTempView("transformationOutputDF")

# COMMAND ----------

dbutils.notebook.exit("transformationOutputDF")