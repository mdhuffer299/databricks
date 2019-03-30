# Databricks notebook source
# MAGIC %md
# MAGIC # Azure Databricks Structured Streaming Example Notebook
# MAGIC ## Notebook Description:
# MAGIC ### Basic example showcasing the ingestion and transformation of a streaming object, using the Spark structured streaming API.
# MAGIC 
# MAGIC We are able to subscribe to various event hubs such as:
# MAGIC * Kafka Streams
# MAGIC * Amazon Kinesis Streams
# MAGIC * Azure Event Hubs
# MAGIC 
# MAGIC We are also able to process static file directories as a stream object.  We are able to read files as a "stream", performing transformations on the stream, and finally writing a stream object back to the Azure Databricks Filesystem.
# MAGIC 
# MAGIC More information about cost-savings by streaming files can be found here:
# MAGIC 
# MAGIC * [Streaming Jobs](https://databricks.com/blog/2017/05/22/running-streaming-jobs-day-10x-cost-savings.html)
# MAGIC 
# MAGIC Structured Streaming is based on the same DataFrame API that we use for regular object reads, but the input table is unbounded.

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls /databricks-datasets/asa/airlines

# COMMAND ----------

# MAGIC %fs
# MAGIC head /databricks-datasets/asa/airlines/1987.csv

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Define the stream object or file schema.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

airlineSchema = StructType([
  StructField("year", IntegerType(), True)
  ,StructField("month", IntegerType(), True)
  ,StructField("dayOfMonth", IntegerType(), True)
  ,StructField("dayOfWeek", IntegerType(), True)
  ,StructField("depTime", StringType(), True)
  ,StructField("crsDepTime", IntegerType(), True)
  ,StructField("arrTime", StringType(), True)
  ,StructField("crsArrTime", IntegerType(), True)
  ,StructField("uniqueCarrier", StringType(), True)
  ,StructField("flightNum", IntegerType(), True)
  ,StructField("tailNum", StringType(), True)
  ,StructField("actualElapsedTime", StringType(), True)
  ,StructField("crsElapsedTime", IntegerType(), True)
  ,StructField("airTime", StringType(), True)
  ,StructField("arrDelay", StringType(), True)
  ,StructField("depDelay", StringType(), True)
  ,StructField("origin", StringType(), True)
  ,StructField("dest", StringType(), True)
  ,StructField("distance", StringType(), True)
  ,StructField("taxiIn", StringType(), True)
  ,StructField("taxiOut", StringType(), True)
  ,StructField("cancelled", IntegerType(), True)
  ,StructField("cancellationCode", StringType(), True)
  ,StructField("diverted", IntegerType(), True)
  ,StructField("carrierDelay", StringType(), True)
  ,StructField("weatherDelay", StringType(), True)
  ,StructField("nasDelay", StringType(), True)
  ,StructField("securityDelay", StringType(), True)
  ,StructField("lateAircraftDelay", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2:  Determine where to write the stream object and checkpoint directory.
# MAGIC 
# MAGIC #### Checkpointing:
# MAGIC We want to create a checkpoint directory per query.  This will aide in the recovery from cluster failure.  Checkpointing can capture both metadata and some of the DataFrame necessary for restarting in case of failure.
# MAGIC 
# MAGIC Note that simple streaming applications without the aforementioned stateful transformations can be run without enabling checkpointing.

# COMMAND ----------

dbutils.fs.mkdirs("/tmp/streamPath")
dbutils.fs.mkdirs("/streamCheck")

airlineReadPath = "/databricks-datasets/asa/airlines/*csv"

checkpointLocation = "/streamCheck"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create the initial streaming object.
# MAGIC 
# MAGIC Similiar to a `spark.read` statement, we assign a DataFrame with a `spark.readStream` call.  We are also able to pass multiple options and file formats to the stream object.
# MAGIC 
# MAGIC No data is processed until an aciton is performed.  The `display(df)` or `df.start()` will initiate the stream.  Once a stream is created, it must be stopped if changes need to be made.  Streams can be stopped by using the `spark.stream.stop()` command.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

airlineStreamDF = (spark
                   .readStream
                   .schema(airlineSchema)
                   .option("maxFilesPerTrigger", 1)
                   .csv(airlineReadPath)
                  ).withColumn("timestamp", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step 4: Perform Transformations on the DataFrame.
# MAGIC 
# MAGIC We are able to process and transform streaming data much like a standard DataFrame.  The only difference is that we must supply a watermark and a window statement.
# MAGIC 
# MAGIC ##### Windows: Specifies a time window that aggregations should be performed on.
# MAGIC 
# MAGIC ##### Watermark: Defines how long we will wait for late data to arrive.
# MAGIC 
# MAGIC In this example, the timestamp is artifically created to show the fucntionality.  In practice, you should use a timestamp indicating the creation of the record.

# COMMAND ----------

from pyspark.sql.functions import avg, window

airlineAggDF = (airlineStreamDF
                .withWatermark("timestamp", "45 seconds")
                .groupBy(window("timestamp", "30 seconds"), "year", "month", "uniqueCarrier","origin", "dest").agg(avg("arrDelay").alias("avgArrDelay"), avg("depDelay").alias("avgDepDelay"))
               )

# Comment out the display function below when running notebook end-to-end
display(airlineAggDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step 5: Write the stream to a target.
# MAGIC 
# MAGIC We can write our data to multiple formats.  We are also able to define the following output modes for a streaming object:
# MAGIC * append - Only the new rows in the streaming DataFrame/Dataset will be written to the sink.
# MAGIC * complete - All the rows in the streaming DataFrame/Dataset will be written to the sink every time these is some updates.
# MAGIC * update - only the rows that were updated in the streaming DataFrame/Dataset will be written to the sink every time there are some updates. If the query doesn’t contain aggregations, it will be equivalent to append mode.
# MAGIC 
# MAGIC Note: `outputMode("append")` must be used when aggregations, windows, and watermarks are performed on the data stream.

# COMMAND ----------

writePath = "/tmp/streamPath"

(airlineAggDF
 .writeStream
 .format("parquet")
 .option("path", writePath)
 .option("checkpointLocation", "/streamCheck")
 .outputMode("append")
 .start()
)

# COMMAND ----------

[q.stop() for q in spark.streams.active]

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now we can check the target directory to verify that data has been written.

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /tmp/streamPath

# COMMAND ----------

df = spark.read.format("parquet").load("/tmp/streamPath/*.snappy.parquet")

display(df)

# COMMAND ----------

dbutils.fs.rm("/tmp/streamPath", True)
dbutils.fs.rm("/streamCheck", True)

# COMMAND ----------

