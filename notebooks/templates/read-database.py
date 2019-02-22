# Databricks notebook source
dbutils.widgets.text("hostName", "No Host Defined")
dbutils.widgets.text("database", "No Database Defined")
dbutils.widgets.text("port", "No Port Defined")
dbutils.widgets.text("user", "No User Defined")
dbutils.widgets.text("password", "No Password Defined")
dbutils.widgets.text("query", "No query Defined")
dbutils.widgets.text("lowerBound", "1")
dbutils.widgets.text("upperBound", "10000")
dbutils.widgets.text("numPartitions", "12")

# COMMAND ----------

hostName = dbutils.widgets.get("hostName")
database = dbutils.widgets.get("database")
port = dbutils.widgets.get("port")
user = dbutils.widgets.get("user")
password = dbutils.widgets.get("password")
query = dbutils.widgets.get("query")
lowerBound = dbutils.widgets.get("lowerBound")
upperBound = dbutils.widgets.get("upperBound")
numPartitions = dbutils.widgets.get("numPartitions")

# COMMAND ----------

connectionUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(hostName, port, database, user, password)

# COMMAND ----------

df = (spark
      .read
      .jdbc(
        url = connectionUrl
        , table = "(" + query + ") AS SUB"
        , lowerBound = lowerBound
        , upperBound = upperBound
        , numPartitions = numPartitions
      )
)

# COMMAND ----------

df.createOrReplaceGlobalTempView("outputDF")
dbutils.notebook.exit("outputDF")

# COMMAND ----------

#dbutils.widgets.removeAll()