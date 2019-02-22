# Databricks notebook source
dbutils.widgets.text("hostName", "No Host Defined")
dbutils.widgets.text("database", "No Database Defined")
dbutils.widgets.text("port", "No Port Defined")
dbutils.widgets.text("user", "No User Defined")
dbutils.widgets.text("password", "No Password Defined")
dbutils.widgets.text("lowerBound", "1")
dbutils.widgets.text("upperBound", "10000")
dbutils.widgets.text("numPartitions", "12")
dbutils.widgets.text("outputTable", "No Output Table Defined")
dbutils.widgets.text("dataframe", "No Dataframe Defined")

# COMMAND ----------

#dbutils.widgets.removeAll()

# COMMAND ----------

hostName = dbutils.widgets.get("hostName")
database = dbutils.widgets.get("database")
port = dbutils.widgets.get("port")
user = dbutils.widgets.get("user")
password = dbutils.widgets.get("password")
lowerBound = dbutils.widgets.get("lowerBound")
upperBound = dbutils.widgets.get("upperBound")
numPartitions = dbutils.widgets.get("numPartitions")
outputTable = dbutils.widgets.get("outputTable")
inputDF = dbutils.widgets.get("dataframe")

# COMMAND ----------

connectionUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(hostName, port, database, user, password)

# COMMAND ----------

global_temp_db = spark.conf.get("spark.sql.globalTempDatabase")
df = table(global_temp_db + "." + inputDF)

# COMMAND ----------

(df
 .write
 .format("jdbc")
 .option("numPartitions", numPartitions)
 .option("url", connectionUrl)
 .option("dbTable", outputTable)
 .mode("overwrite")
 .save()
)

# COMMAND ----------

dbutils.notebook.exit("Notebook Completed, data was loaded to: " + database + "." + outputTable)