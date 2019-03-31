# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Notebook Introduction:
# MAGIC #### This notebook is used to explain and demonstrate the basic functionality of a notebook.
# MAGIC 
# MAGIC ##### What is a notebook?
# MAGIC * A notebook is a web-based interface to a document that contains runnable code, visualizations, and narrative text. Notebooks are one interface for interacting with Databricks.
# MAGIC 
# MAGIC ##### Why use a notebook?
# MAGIC * Notebooks allow you to effectively interact with a Databricks Cluster.  We are able to securely build, share for collaboration, and schedule notebooks to execute Databricks workflows.  A notebook allows you to execute data extracts, transformations, and loads using multiple languages through the magic commands.
# MAGIC 
# MAGIC ##### How to execute cells?
# MAGIC * Prior to executing any cells, we must first create a cluster and attach a notebook to the cluster.
# MAGIC * When you attach a notebook to a cluster, Databricks creates an execution context. An execution context contains the state for a REPL environment for each supported programming language: Python, R, Scala, and SQL. When you run a cell in a notebook, the command is dispatched to the appropriate language REPL environment and run.
# MAGIC * A cluster has a maximum number of execution contexts (145). Once the number of execution contexts has reached this threshold, you cannot attach a notebook to the cluster or create a new execution context.
# MAGIC 
# MAGIC ##### What is a DataFrame?
# MAGIC * The Apache Spark DataFrame API provides a rich set of functions (select columns, filter, join, aggregate, and so on) that allow you to solve common data analysis problems efficiently. DataFrames also allow you to intermix operations seamlessly with custom Python, R, Scala, and SQL code.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val scala_df = (spark
# MAGIC           .read
# MAGIC           .json("/databricks-datasets/iot/iot_devices.json")
# MAGIC          )
# MAGIC 
# MAGIC scala_df.show()

# COMMAND ----------

# MAGIC %r
# MAGIC library(SparkR)
# MAGIC 
# MAGIC r_df <- read.json("/databricks-datasets/iot/iot_devices.json")
# MAGIC 
# MAGIC showDF(r_df)

# COMMAND ----------

# MAGIC %python
# MAGIC python_df = (spark
# MAGIC       .read
# MAGIC       .json("/databricks-datasets/iot/iot_devices.json")
# MAGIC      )
# MAGIC 
# MAGIC display(python_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Visualizations
# MAGIC Databricks notebooks offers the ability to create visualizations on the same dataframes when the `display(df)` function is called within a cell.

# COMMAND ----------

plotDF  = (python_df
           .select("temp", "device_id")
          )

display(plotDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Additional visualization libraries
# MAGIC You are also able to leverage other visualization libraries such as `matplotlib` and `ggplot` in your Databricks notebooks.

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np

x = np.array(plotDF.select("device_id").take(50))
y = np.array(plotDF.select("temp").take(50))

fig, ax = plt.subplots()
ax.plot(x, y, 'ro')
ax.plot(x,y, 'k-')

display(fig)

# COMMAND ----------

