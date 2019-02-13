# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Capstone Project: Custom Transformations, Aggregating and Loading
# MAGIC 
# MAGIC The goal of this project is to populate aggregate tables using Twitter data.  In the process, you write custom User Defined Functions (UDFs), aggregate daily most trafficked domains, join new records to a lookup table, and load to a target database.
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers
# MAGIC * Additional Audiences: Data Scientists and Data Pipeline Engineers
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: Chrome or Firefox
# MAGIC * Lesson: <a href="$./03-User-Defined-Functions">User Defined Functions</a> 
# MAGIC * Lesson: <a href="$./05-Joins-and-Lookup-Tables">Joins and Lookup Tables</a> 
# MAGIC * Lesson: <a href="$./06-Database-Writes">Database Writes</a> 
# MAGIC 
# MAGIC ## Instructions
# MAGIC 
# MAGIC The Capstone work for the previous course in this series (ETL: Part 1) defined a schema and created tables to populate a relational mode. In this capstone project you take the project further.
# MAGIC 
# MAGIC In this project you ETL JSON Twitter data to build aggregate tables that monitor trending websites and hashtags and filter malicious users using historical data.  Use these four exercises to achieve this goal:<br><br>
# MAGIC 
# MAGIC 1. **Parse tweeted URLs** using a custom UDF
# MAGIC 2. **Compute aggregate statistics** of most tweeted websites and hashtags by day
# MAGIC 3. **Join new data** to an existing dataset of malicious users
# MAGIC 4. **Load records** into a target database

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Parse Tweeted URLs
# MAGIC 
# MAGIC Some tweets in the dataset contain links to other websites.  Import and explore the dataset using the provided schema.  Then, parse the domain name from these URLs using a custom UDF.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Import and Explore
# MAGIC 
# MAGIC The following is the schema created as part of the capstone project for ETL Part 1.  
# MAGIC Run the following cell and then use this schema to import one file of the Twitter data.

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, ArrayType, StringType, IntegerType, LongType
from pyspark.sql.functions import col

fullTweetSchema = StructType([
  StructField("id", LongType(), True),
  StructField("user", StructType([
    StructField("id", LongType(), True),
    StructField("screen_name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("friends_count", IntegerType(), True),
    StructField("followers_count", IntegerType(), True),
    StructField("description", StringType(), True)
  ]), True),
  StructField("entities", StructType([
    StructField("hashtags", ArrayType(
      StructType([
        StructField("text", StringType(), True)
      ]),
    ), True),
    StructField("urls", ArrayType(
      StructType([
        StructField("url", StringType(), True),
        StructField("expanded_url", StringType(), True),
        StructField("display_url", StringType(), True)
      ]),
    ), True)
  ]), True),
  StructField("lang", StringType(), True),
  StructField("text", StringType(), True),
  StructField("created_at", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC Import one file of the JSON data located at `/mnt/training/twitter/firehose/2018/01/08/18/twitterstream-1-2018-01-08-18-48-00-bcf3d615-9c04-44ec-aac9-25f966490aa4` using the schema.  Be sure to do the following:<br><br>
# MAGIC 
# MAGIC * Save the result to `tweetDF`
# MAGIC * Apply the schema `fullTweetSchema`
# MAGIC * Filter out null values from the `id` column

# COMMAND ----------

path = "/mnt/training/twitter/firehose/2018/01/08/18/twitterstream-1-2018-01-08-18-48-00-bcf3d615-9c04-44ec-aac9-25f966490aa4"
tweetDF = (spark.read
           .schema(fullTweetSchema)
           .json(path)
          ).where(col("id").isNotNull())

display(tweetDF)

# COMMAND ----------

# TEST - Run this cell to test your solution
dbTest("ET2-P-08-01-01", 1491, tweetDF.count())
dbTest("ET2-P-08-01-02", True, "text" in tweetDF.columns and "id" in tweetDF.columns)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 2: Write a UDF to Parse URLs
# MAGIC 
# MAGIC The Python regular expression library `re` allows you to define a set of rules of a string you want to match. In this case, parse just the domain name in the string for the URL of a link in a Tweet. Take a look at the following example:
# MAGIC 
# MAGIC ```
# MAGIC import re
# MAGIC 
# MAGIC URL = "https://www.databricks.com/"
# MAGIC pattern = re.compile(r"https?://(www\.)?([^/#?]+).*$")
# MAGIC match = pattern.search(URL)
# MAGIC print("The string {} matched {}".format(URL, match.group(2)))
# MAGIC ```
# MAGIC 
# MAGIC This code prints `The string https://www.databricks.com/ matched spark.apache.org`. **Wrap this code into a function named `getDomain` that takes a parameter `URL` and returns the matched string.**
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> <a href="https://docs.python.org/3/howto/regex.html" target="_blank">You can find more on the `re` library here.</a>

# COMMAND ----------

import re

def getDomain(URL):
  pattern = re.compile(r"https?://(www\.)?([^/#?]+).*$")
  match = pattern.search(URL)
  if match:
    return(match.group(2))

URL = "https://www.databricks.com/"
print("The string {} matched {}".format(URL, getDomain(URL)))

# COMMAND ----------

# TEST - Run this cell to test your solution
dbTest("ET2-P-08-02-01", "databricks.com",  getDomain("https://www.databricks.com/"))

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Test and Register the UDF
# MAGIC 
# MAGIC Now that the function works with a single URL, confirm that it works on different URL formats.

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to test your function further.

# COMMAND ----------

urls = [
  "https://www.databricks.com/",
  "https://databricks.com/",
  "https://databricks.com/training-overview/training-self-paced",
  "http://www.databricks.com/",
  "http://databricks.com/",
  "http://databricks.com/training-overview/training-self-paced",
  "http://www.apache.org/",
  "http://spark.apache.org/docs/latest/"
]

for url in urls:
  print(getDomain(url))

# COMMAND ----------

# MAGIC %md
# MAGIC Register the UDF as `getDomainUDF`.

# COMMAND ----------

from pyspark.sql.types import StringType

getDomainUDF = spark.udf.register("getDomainSQLUDF", getDomain, StringType())

# COMMAND ----------

# TEST - Run this cell to test your solution
dbTest("ET2-P-08-03-01", True, bool(getDomainUDF))

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Apply the UDF
# MAGIC 
# MAGIC Create a dataframe called `urlDF` that has three columns:<br><br>
# MAGIC 
# MAGIC 1. `URL`: The URL's from `tweetDF` (located in `entities.urls.expanded_url`) 
# MAGIC 2. `parsedURL`: The UDF applied to the column `URL`
# MAGIC 3. `created_at`
# MAGIC 
# MAGIC There can be zero, one, or many URLs in any tweet.  For this step, use the `explode` function, which takes an array like URLs and returns one row for each value in the array.
# MAGIC <a href="http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=explode#pyspark.sql.functions.explode" target="_blank">See the documents here for details.</a>

# COMMAND ----------

from pyspark.sql.functions import explode

urlDF = (tweetDF
         .withColumn("URL", explode("entities.urls.expanded_url"))
         .select("URL","created_at")
         .withColumn("parsedURL", getDomainUDF("URL"))
        )

display(urlDF)

# COMMAND ----------

# TEST - Run this cell to test your solution
cols = urlDF.columns
sample = urlDF.first()

dbTest("ET2-P-08-04-01", True, "URL" in cols and "parsedURL" in cols and "created_at" in cols)
dbTest("ET2-P-08-04-02", "https://www.youtube.com/watch?v=b4iz9nZPzAA", sample["URL"])
dbTest("ET2-P-08-04-03", "Mon Jan 08 18:47:59 +0000 2018", sample["created_at"])
dbTest("ET2-P-08-04-04", "youtube.com", sample["parsedURL"])

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Compute Aggregate Statistics
# MAGIC 
# MAGIC Calculate top trending 10 URLs by hour.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Parse the Timestamp
# MAGIC 
# MAGIC Create a DataFrame `urlWithTimestampDF` that includes the following columns:<br><br>
# MAGIC 
# MAGIC * `URL`
# MAGIC * `parsedURL`
# MAGIC * `timestamp`
# MAGIC * `hour`
# MAGIC 
# MAGIC Import `unix_timestamp` and `hour` from the `functions` module and `TimestampType` from the types `module`. To parse the `create_at` field, use `unix_timestamp` with the format `EEE MMM dd HH:mm:ss ZZZZZ yyyy`.

# COMMAND ----------

display(urlDF)

# COMMAND ----------

from pyspark.sql.functions import unix_timestamp, hour, col
from pyspark.sql.types import TimestampType

"""
URL
parsedURL
timestamp
hour
"""

timestampFormat = "EEE MMM dd HH:mm:ss ZZZZZ yyyy"
urlWithTimestampDF = (urlDF.select
                      (col("URL")
                       ,col("parsedURL")
                       ,unix_timestamp(col("created_at"), format = timestampFormat).cast(TimestampType()).alias("timestamp")
                       ,hour(unix_timestamp(col("created_at"), format = timestampFormat).cast(TimestampType())).alias("hour"))
                     )

display(urlWithTimestampDF)

# COMMAND ----------

# TEST - Run this cell to test your solution
cols = urlWithTimestampDF.columns
sample = urlWithTimestampDF.first()

dbTest("ET2-P-08-05-01", True, "URL" in cols and "parsedURL" in cols and "timestamp" in cols and "hour" in cols)
dbTest("ET2-P-08-05-02", 18, sample["hour"])

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Calculate Trending URLs
# MAGIC 
# MAGIC Create a DataFrame `urlTrendsDF` that looks at the top 10 hourly counts of domain names and includes the following columns:<br><br>
# MAGIC 
# MAGIC * `hour`
# MAGIC * `parsedURL`
# MAGIC * `count`
# MAGIC 
# MAGIC The result should sort `hour` in ascending order and `count` in descending order.

# COMMAND ----------

from pyspark.sql.functions import col

urlTrendsDF = (urlWithTimestampDF
               .select("hour","parsedURL")
               .groupBy("hour","parsedURL").count()
               .orderBy(["count","hour"], ascending = [0,1])
              )

display(urlTrendsDF)

# COMMAND ----------

# TEST - Run this cell to test your solution
cols = urlTrendsDF.columns
sample = urlTrendsDF.first()

dbTest("ET2-P-08-06-01", True, "hour" in cols and "parsedURL" in cols and "count" in cols)
dbTest("ET2-P-08-06-02", 18, sample["hour"])
dbTest("ET2-P-08-06-03", "twitter.com", sample["parsedURL"])
dbTest("ET2-P-08-06-04", 159, sample["count"])

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Join New Data
# MAGIC 
# MAGIC Filter out bad users.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Import Table of Bad Actors
# MAGIC 
# MAGIC Create the DataFrame `badActorsDF`, a list of bad actors that sits in `/mnt/training/twitter/supplemental/badactors.parquet`.

# COMMAND ----------

badActorsDF = (spark.read
               .parquet("/mnt/training/twitter/supplemental/badactors.parquet")
              )

display(badActorsDF)

# COMMAND ----------

# TEST - Run this cell to test your solution
cols = badActorsDF.columns
sample = badActorsDF.first()

dbTest("ET2-P-08-07-01", True, "userID" in cols and "screenName" in cols)
dbTest("ET2-P-08-07-02", 4875602384, sample["userID"])
dbTest("ET2-P-08-07-03", "cris_silvag1", sample["screenName"])

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Add a Column for Bad Actors
# MAGIC 
# MAGIC Add a new column to `tweetDF` called `maliciousAcct` with `true` if the user is in `badActorsDF`.  Save the results to `tweetWithMaliciousDF`.  Remember to do a left join of the malicious accounts on `tweetDF`.

# COMMAND ----------

tweetWithMaliciousDF = (tweetDF
                        .join(badActorsDF, tweetDF.user.id == badActorsDF.userID, 'left')
                        .withColumn("maliciousAcct", col("userID").isNotNull())
                       )

display(tweetWithMaliciousDF)

# COMMAND ----------

# TEST - Run this cell to test your solution
cols = tweetWithMaliciousDF.columns
sample = tweetWithMaliciousDF.first()

dbTest("ET2-P-08-08-01", True, "maliciousAcct" in cols and "id" in cols)
dbTest("ET2-P-08-08-02", 950438954272096257, sample["id"])
dbTest("ET2-P-08-08-03", False, sample["maliciousAcct"])

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Load Records
# MAGIC 
# MAGIC Transform your two DataFrames to 4 partitions and save the results to the following endpoints:
# MAGIC 
# MAGIC | DataFrame              | Endpoint                            |
# MAGIC |:-----------------------|:------------------------------------|
# MAGIC | `urlTrendsDF`          | `userhome + /tmp/urlTrends.parquet`            |
# MAGIC | `tweetWithMaliciousDF` | `userhome + /tmp/tweetWithMaliciousDF.parquet` |

# COMMAND ----------

urlTrendsDF.repartition(4).write.mode("overwrite").parquet(userhome + "/tmp/urlTrends.parquet")
tweetWithMaliciousDF.repartition(4).write.mode("overwrite").parquet(userhome + "/tmp/tweetWithMaliciousDF.parquet")

# COMMAND ----------

# TEST - Run this cell to test your solution
urlTrendsDFTemp = spark.read.parquet(userhome + "/tmp/urlTrends.parquet")
tweetWithMaliciousDFTemp = spark.read.parquet(userhome + "/tmp/tweetWithMaliciousDF.parquet")

dbTest("ET2-P-08-09-01", 4, urlTrendsDFTemp.rdd.getNumPartitions())
dbTest("ET2-P-08-09-02", 4, tweetWithMaliciousDFTemp.rdd.getNumPartitions())

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## IMPORTANT Next Steps
# MAGIC * Please complete the <a href="https://www.surveymonkey.com/r/VYGM9TD" target="_blank">short feedback survey</a>.  Your input is extremely important and shapes future course development.
# MAGIC * Congratulations, you have completed ETL Part 2!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>