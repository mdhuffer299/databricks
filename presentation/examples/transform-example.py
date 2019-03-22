# Databricks notebook source
# MAGIC %md
# MAGIC # Azure Databricks Transformation Example Notebook
# MAGIC ## Notebook Description:
# MAGIC ### Basic example showcasing multiple features of an Azure Databricks notebook workspace as well as core Databricks functionality such as reading data, transforming data, and writing data back to the Databricks File System.
# MAGIC 
# MAGIC ##### Spark provides multiple high level APIs that allow you to interact with data using multiple programming languages.
# MAGIC * Java
# MAGIC * Scala
# MAGIC * Python
# MAGIC * R
# MAGIC 
# MAGIC ##### Within a Databricks notebook, you can create multiple individual cells using differnt interpreters.  This allows you to pass and manipulate data between different programming languages.  To do so, you can override the primary notebook language by specifying the language magic command %language
# MAGIC * %python
# MAGIC * %r
# MAGIC * %scala
# MAGIC * %sql
# MAGIC 
# MAGIC You are also able to interact directly with the Databricks File System using either the %fs magic or dbutils.fs functions.  
# MAGIC 
# MAGIC In addition, you are able to execute shell commands using the %sh magic option.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Exploring the underlying directories and file structure.
# MAGIC 
# MAGIC With this step, we are trying to get an idea of the number of files, the format, its strcuture, and possible data types.
# MAGIC 
# MAGIC We are able to run simple file system commands such as `ls` or `head` by specifiying the %fs magic.

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /databricks-datasets

# COMMAND ----------

# MAGIC %fs
# MAGIC head /databricks-datasets/iot/iot_devices.json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Reading in our data to a Spark DataFrame
# MAGIC 
# MAGIC As we can see from the %fs head command above, we have a json file format that contains 15 columns with various iot-device measuresments such as location, name, temperature, etc...
# MAGIC 
# MAGIC Now that we have a basic understanding of our file, we can begin to read the file into a Spark DataFrame.  We can supply multiple options to the read statement, but an important decision is whether or not we should define a schema or use the infer option.
# MAGIC 
# MAGIC #### Schema Options:
# MAGIC * Infer Schema
# MAGIC * Pre-define Schema
# MAGIC 
# MAGIC When using the Infer Schema option, Spark will do a full scan of the file to understand the schema prior to create the read object.
# MAGIC 
# MAGIC Best practice would be to define the DataFrame Schema whenever possible to improve processing times.
# MAGIC 
# MAGIC #### Additional Read Options:
# MAGIC * Define a corrupt record process
# MAGIC * Header options
# MAGIC * Delimiters options
# MAGIC * File format options

# COMMAND ----------

dfInferSchema = (spark
      .read
      .option("inferSchema", "True")
      .json("/databricks-datasets/iot/iot_devices.json")
     )

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

iotSchema = StructType([
  StructField("device_id", LongType())
  , StructField("device_name", StringType())
  , StructField("ip", StringType())
  , StructField("cca2", StringType())
  , StructField("cca3", StringType())
  , StructField("cn", StringType())
  , StructField("latitude", DoubleType())
  , StructField("longitude", DoubleType())
  , StructField("scale", StringType())
  , StructField("temp", LongType())
  , StructField("humidity", LongType())
  , StructField("battery_level", LongType())
  , StructField("c02_level", LongType())
  , StructField("lcd", StringType())
  , StructField("timestamp", LongType())
])

dfSchema = (spark
      .read
      .schema(iotSchema)
      .json("/databricks-datasets/iot/iot_devices.json")
     )

# COMMAND ----------

display(dfSchema)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step 3: Create some transformation logic
# MAGIC 
# MAGIC Transformations in Spark can be as simple as adding a new indicator column based on condition, to as complex as explodding complex data types.
# MAGIC 
# MAGIC These transformations can also be performed in any of the programming languages available in the Databricks platform.
# MAGIC 
# MAGIC The below cell is importing multiple sql functions from the pyspark.sql library.  These functions will allow us to parse the raw timestamp column and get additional time attributes for future analysis.

# COMMAND ----------

from pyspark.sql.functions import from_unixtime, unix_timestamp, col, hour, month, year, dayofmonth
from pyspark.sql.types import TimestampType

iotDeviceDF = (dfSchema
         .select("battery_level"
                 , "cn"
                 , "device_id"
                 , "device_name"
                 , "humidity"
                 , "temp"
                 , "timestamp"
                 , hour(from_unixtime(col("timestamp")/1000, format = 'yyyy-MM-dd HH:mm:ss')).alias("hour")
                 , dayofmonth(from_unixtime(col("timestamp")/1000, format = 'yyyy-MM-dd HH:mm:ss')).alias("day")
                 , month(from_unixtime(col("timestamp")/1000, format = 'yyyy-MM-dd HH:mm:ss')).alias("month")
                 , year(from_unixtime(col("timestamp")/1000, format = 'yyyy-MM-dd HH:mm:ss')).alias("year"))
         .withColumn("time", from_unixtime(col("timestamp")/1000, format = 'yyyy-MM-dd HH:mm:ss'))
        )

display(iotDeviceDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions
# MAGIC While the preferred and recommended approach is to leverage built-in functions, we will sometimes need to create our own functions that can need to be applied repeatedly. 
# MAGIC 
# MAGIC Databricks enables this with User Defined Functions (UDFs).  These UDFs should be created in Scala when possible, since Python UDFs has some additional overhead during the execution process.
# MAGIC 
# MAGIC UDFs can be defined once, and exposed to both the original programming language (i.e. Python), as well as the SQL context.  This enables you to use the same function in both instances.

# COMMAND ----------

def tempConversion(scale, temp):
  if(str(scale) == "Celsius"):
    fahrenheitTemp = temp*(9/5) + 32
    kelvinTemp = temp + 273.15
  else:
    fahrenheitTemp = temp
    kelvinTemp = temp
  return(fahrenheitTemp, kelvinTemp)

tempConversion("Celsius", 10)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, FloatType

tempConversionSchema = StructType([
  StructField("fahrenheitTemp", FloatType())
  , StructField("kelvinTemp", FloatType())
])

# COMMAND ----------

from pyspark.sql.types import FloatType

tempConversionUDF = spark.udf.register("tempConversionSQLUDF", tempConversion, tempConversionSchema)

# COMMAND ----------

df = (dfSchema
      .withColumn("scaleConv", tempConversionUDF("scale", "temp"))
     ).select("*","scaleConv.*")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We are also able to create temporary or persisted SQL like tables of our Spark DataFrames using a few different commands.
# MAGIC 
# MAGIC `df.createOrReplaceTempView("tableName")` - creates a local table that exists for your current cluster/compute context
# MAGIC 
# MAGIC `df.write.saveAsTable("tableName")` - creates a global table that is accessible to multiple clusters running in your workspace.
# MAGIC 
# MAGIC `CREATE TABLE ...` - allows you to create global tables using SQL syntax
# MAGIC 
# MAGIC This allows us to operate on our data using a comman SQL syntax.  This is also an option to pass data between notebooks during pipeline creations.

# COMMAND ----------

iotDeviceDF.createOrReplaceTempView("iotDeviceDF")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS iotDeviceTable;
# MAGIC 
# MAGIC CREATE TABLE iotDeviceTable AS
# MAGIC SELECT
# MAGIC   cn AS country_name
# MAGIC   , hour
# MAGIC   , day
# MAGIC   , month
# MAGIC   , year
# MAGIC   , AVG(humidity) AS avg_country_humidty
# MAGIC   , AVG(temp) AS avg_country_temp_celcius
# MAGIC   , AVG((temp*(9/5) + 32)) AS avg_country_temp_farhenheit
# MAGIC   , AVG((temp + 273.15)) AS avg_country_temp_kelvin
# MAGIC FROM iotDeviceDF
# MAGIC GROUP BY
# MAGIC   cn
# MAGIC   , hour
# MAGIC   , day
# MAGIC   , month
# MAGIC   , year;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM iotDeviceTable

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We are also able to write SQL tables back to dataframes for future processing.
# MAGIC 
# MAGIC To do so you can use the `sqlContext.table("tableName")` syntax

# COMMAND ----------

iotTableDF = sqlContext.table("iotDeviceTable")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step 4: Writing Data
# MAGIC 
# MAGIC Similiar to the read operations, Spark enables us to write multiple data formats to a wide variety of target locations.
# MAGIC 
# MAGIC These files can be written to vairous distributed file systems (Azure Blob, S3, HDFS, Databricks FS) as well as target databases (Azure SQL DB/DW, Redshift, Snowflake)
# MAGIC 
# MAGIC When writing to databses, it is recommended to leverage a native connector over the generic JDBC connector.  Especially, when you are dealing with bulk loads.  The JDBC connector, uses a row by row insert method and can be taxing on the target system.

# COMMAND ----------

# Create generic dataframe file
dfSchema.write.mode("overwrite").format("parquet").partitionBy("cn").save("/tmp/genericIot")

# Create Databricks Delta dataframe file
dfSchema.write.mode("overwrite").format("delta").partitionBy("cn").save("/tmp/deltaIot")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Quick Databricks Delta Example
# MAGIC 
# MAGIC Databricks Delta provides an improvement over traditional Databricks parquet tables.  It combats some of the original issues that come with managing Big Data in a schema on read architecture.  Some of the key features are:
# MAGIC * ACID Transactions
# MAGIC * Efficient Upserts
# MAGIC * Fast Streaming
# MAGIC * Optimized Layouts
# MAGIC * Schema Enforcement
# MAGIC * Data Versioning
# MAGIC 
# MAGIC More information about Databricks Delta can be found here:
# MAGIC [Databricks Delta](https://docs.databricks.com/delta/index.html)

# COMMAND ----------

# Create Genric Table
spark.sql("""
  DROP TABLE IF EXISTS generic_iot
""")
spark.sql("""
  CREATE TABLE generic_iot
  USING PARQUET
  OPTIONS (path = '{}')
""".format("/tmp/genericIot"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM generic_iot

# COMMAND ----------

# MAGIC %sql
# MAGIC MSCK REPAIR TABLE generic_iot;
# MAGIC 
# MAGIC SELECT COUNT(*) FROM generic_iot;

# COMMAND ----------

spark.sql("""
  DROP TABLE IF EXISTS iot_device_delta
""")
spark.sql("""
  CREATE TABLE iot_device_delta
  USING DELTA
  LOCATION '{}'
""".format("/tmp/deltaIot"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM iot_device_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE generic_iot;
# MAGIC DROP TABLE iot_device_delta;
# MAGIC DROP TABLE iotdevicetable;
# MAGIC DROP TABLE iotglobaltable;