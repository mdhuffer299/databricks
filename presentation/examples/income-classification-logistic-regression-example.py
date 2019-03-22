# Databricks notebook source
# MAGIC %fs 
# MAGIC ls /databricks-datasets/adult/adult.data

# COMMAND ----------

df = (spark
      .read
      .option("inferSchema", "True")
      .csv("/databricks-datasets/adult/adult.data")
     )

display(df)

# COMMAND ----------

from functools import reduce

oldColumns = df.schema.names
newColumns = ["age", "workclass", "fnlwgt", "education", "education_num", "marital_status", "occupation", "relationship", "race", "sex", "capital_gain", "capital_loss", "hours_per_week", "native_country", "income"]

df = reduce(lambda df, idx: df.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), df)

display(df)

# COMMAND ----------

tableDefinitions = df.dtypes

dataframeColumns = df.columns

categoricalColumns = []
numericColumns = []

labelColumn = "income"

for tableTup in tableDefinitions:
  if tableTup[1] == 'int' or tableTup[1] == 'double':
    numericColumns.append(tableTup[0])
  elif tableTup[1] == 'string':
    categoricalColumns.append(tableTup[0])
  else:
    None
    
if labelColumn in categoricalColumns:
  categoricalColumns.remove(labelColumn)
elif labelColumn in numericColumns:
  numericColumns.remove(labelColumn)
else:
  None

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler

stages = []

for categoricalCol in categoricalColumns:
  stringIndexer = StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + "Index")
  encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols = [categoricalCol + "classVec"])
  
  stages += [stringIndexer, encoder]

# COMMAND ----------

label_stringIdx = StringIndexer(inputCol = labelColumn, outputCol = "label")
stages += [label_stringIdx]

# COMMAND ----------

assemblerInputs = [c + "classVec" for c in categoricalColumns] + numericColumns
assembler = VectorAssembler(inputCols = assemblerInputs, outputCol = "features")
stages += [assembler]

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression

partialPipeline = Pipeline().setStages(stages)
pipelineModel = partialPipeline.fit(df)
preppedDataDF = pipelineModel.transform(df)

# COMMAND ----------

(trainingData, testData) = preppedDataDF.randomSplit([0.7, 0.3], seed = 100)
print(trainingData.count())
print(testData.count())

# COMMAND ----------

lrModel = LogisticRegression().fit(trainingData)

# COMMAND ----------

predictionsDF = (lrModel.transform(testData)).select("income", "label", "prediction", "probability")

# COMMAND ----------

predictionsDF.registerTempTable("incomePredictionsOutputDF")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM incomePredictionsOutputDF

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT 
# MAGIC   CASE
# MAGIC     WHEN label = prediction THEN 'Yes'
# MAGIC     ELSE 'No'
# MAGIC   END AS correctPrediction
# MAGIC FROM incomePredictionsOutputDF

# COMMAND ----------

