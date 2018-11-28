# Databricks notebook source
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.types import DoubleType, ArrayType

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/nfl-data/pbp-get/test/nflFull

# COMMAND ----------

nflDF = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/nfl-data/pbp-get/test/nflFull/*.csv")

# COMMAND ----------

nflClean.registerTempTable("tempDF")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT distinct(play_type) FROM tempDF

# COMMAND ----------

columns = ["game_id", "play_id", "drive", "qtr", "down", "game_seconds_remaining", "side_of_field", "yrdln", "yardline_100", "ydstogo", "posteam", "defteam", "play_type","posteam_score", "defteam_score", "score_differential", "home_team", "away_team", "season"]

nflSubset = nflDF[columns]
nflSubset = nflSubset.where((nflSubset['play_type'] == 'pass') | (nflSubset['play_type'] == 'run'))

nflClean = nflSubset.dropna()

# COMMAND ----------

intColumns = ["down", "game_seconds_remaining", "yrdln", "yardline_100", "posteam_score", "defteam_score", "score_differential"]

for col in intColumns:
  nflClean = nflClean.withColumn(col, nflClean[col].cast(DoubleType()))
  
nflClean = nflClean.na.fill(0)

# COMMAND ----------

labelIndexer = StringIndexer(inputCol = "play_type", outputCol = "indexedLabel").fit(nflClean)

# Converting all categorical variables into factor indexes
# All string values must be in a numerical format, unlike R, you are not able to create STRING "Factor" levels
PosTeamIndexer = StringIndexer(inputCol = "posteam", outputCol = "indexedPosTeam")
DefTeamIndexer = StringIndexer(inputCol = "defteam", outputCol = "indexedDefTeam")
HomeTeamIndexer = StringIndexer(inputCol = "home_team", outputCol = "indexedHomeTeam")
AwayTeamIndexer = StringIndexer(inputCol = "away_team", outputCol = "indexedAwayTeam")
SideOfFieldIndexer = StringIndexer(inputCol = "side_of_field", outputCol = "indexedSideOfField")

# Issue with indexer and null values
# You must remove all null values from the dataset prior to fitting the algorithm
# Below creates a vector that consists of all values for a given record
# Vector is fedd into the algorithm during the training and test set
assembler = VectorAssembler(inputCols = ["game_id", "play_id", "drive", "qtr", "down", "game_seconds_remaining", "indexedSideOfField", "yrdln", "yardline_100", "ydstogo", "indexedPosTeam", "indexedDefTeam", "posteam_score", "defteam_score", "score_differential", "indexedHomeTeam", "indexedAwayTeam", "season"], outputCol = "indexedFeatures")

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
predictions.select("predictedLabel", "play_type", "indexedFeatures").show(5)

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