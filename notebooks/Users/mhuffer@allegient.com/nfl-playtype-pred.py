# Databricks notebook source
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.types import DoubleType, ArrayType

# COMMAND ----------

# MAGIC %run /Users/mhuffer@allegient.com/nfl-dataset

# COMMAND ----------

columns = ["GameID", "play_id", "Drive", "qtr", "down", "TimeSecs", "PlayTimeDiff", "SideofField", "yrdln", "yrdline100", "ydstogo", "posteam", "DefensiveTeam", "PlayType","PosTeamScore", "DefTeamScore", "AbsScoreDiff", "HomeTeam", "AwayTeam", "Season"]

nflSubset = nflDF[columns]
nflSubset = nflSubset.where((nflSubset['PlayType'] == 'Pass') | (nflSubset['PlayType'] == 'Run'))

nflClean = nflSubset.dropna()

# COMMAND ----------

intColumns = ["down", "TimeSecs", "PlayTimeDiff", "yrdln", "yrdline100", "PosTeamScore", "DefTeamScore", "AbsScoreDiff"]

for col in intColumns:
  nflClean = nflClean.withColumn(col, nflClean[col].cast(DoubleType()))
  
nflClean = nflClean.na.fill(0)

# COMMAND ----------

labelIndexer = StringIndexer(inputCol = "PlayType", outputCol = "indexedLabel").fit(nflClean)

# Converting all categorical variables into factor indexes
# All string values must be in a numerical format, unlike R, you are not able to create STRING "Factor" levels
PosTeamIndexer = StringIndexer(inputCol = "posteam", outputCol = "indexedPosTeam")
DefTeamIndexer = StringIndexer(inputCol = "DefensiveTeam", outputCol = "indexedDefTeam")
HomeTeamIndexer = StringIndexer(inputCol = "HomeTeam", outputCol = "indexedHomeTeam")
AwayTeamIndexer = StringIndexer(inputCol = "AwayTeam", outputCol = "indexedAwayTeam")
SideOfFieldIndexer = StringIndexer(inputCol = "SideofField", outputCol = "indexedSideOfField")

# Issue with indexer and null values
# You must remove all null values from the dataset prior to fitting the algorithm
# Below creates a vector that consists of all values for a given record
# Vector is fedd into the algorithm during the training and test set
assembler = VectorAssembler(inputCols = ["GameID", "play_id", "Drive", "qtr", "down", "TimeSecs", "PlayTimeDiff", "indexedSideOfField", "yrdln", "yrdline100", "ydstogo", "indexedPosTeam", "indexedDefTeam", "PosTeamScore", "DefTeamScore", "PosTeamScore", "DefTeamScore", "indexedHomeTeam", "indexedAwayTeam", "Season"], outputCol = "indexedFeatures")

# Create the 70/30 split for train and test data sets
(trainData, testData) = nflClean.randomSplit([0.7, 0.3])

# Train the model using the defined train data set
# Max number of bins in forest increased from default = 32 to 40 to ensure that Number of categorical variables indexed above can be split upon
rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures", numTrees = 10, maxBins = 40)

# Return the string value for the predicted label index
# 0 for Pass and 1 for Run
labelConverter = IndexToString(inputCol = "prediction", outputCol = "predictedLabel", labels=labelIndexer.labels)

# Execution of the various steps in the process above
rfPipeline = Pipeline(stages=[labelIndexer, PosTeamIndexer, DefTeamIndexer, HomeTeamIndexer, AwayTeamIndexer, SideOfFieldIndexer, assembler, rf, labelConverter])

# Prior to this everything is not actaully evaluated due to Spark's Lazy Evaluation
model = rfPipeline.fit(trainData)

# Invokes all of the previous steps identified in the pipeline on the test data split
# outputs a dataframe called "predictions"
predictions = model.transform(testData)

# Print out the predicted Play Type, Actual Play Type, and the vector of indexed features
predictions.select("predictedLabel", "PlayType", "indexedFeatures").show(5)

# Determine the accuracy of the model
# Can specify other evaluation metrics
evaluator = MulticlassClassificationEvaluator(labelCol="indexedLabel", predictionCol = "prediction", metricName="accuracy")

# Calculate the test error
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g" % (1.0 - accuracy))

rfModel = model.stages[2]
print(rfModel)

# COMMAND ----------

predictions.select("indexedLabel","prediction","predictedLabel").show(5)

# COMMAND ----------

rfPipeline.write().overwrite().save("nfl-data/pipelines")

# COMMAND ----------

rfModel.write().overwrite().save("/nfl-data/models")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /nfl-data/pipelines/stages

# COMMAND ----------

# Currently working on creating a UDF to disassemble the vector that contains the probability to create upper and lower bounds.
# Write CSV does not allow the the ability to write complext types
predDF = predictions.drop("indexedFeatures", "rawPrediction", "probability")


# COMMAND ----------

predDF.registerTempTable("predDF")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM predDF
# MAGIC LIMIT 2

# COMMAND ----------

predDF.write.csv('/nfl-data/playTypePredictions.csv')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls  /nfl-data/playTypePredictions.csv