# Databricks notebook source
# MAGIC %md
# MAGIC # Azure Databricks Mchine Learning Example Notebook
# MAGIC ## Notebook Description:
# MAGIC ### Basic example showcasing the pipeline creation of a Machine Learning Model.
# MAGIC 
# MAGIC Typically when running machine learning algorithms, it involves a sequence of tasks including pre-processing, feature extraction, model fitting, and validation stages. For example, when classifying text documents might involve text segmentation and cleaning, extracting features, and training a classification model with cross-validation. Though there are many libraries we can use for each stage, connecting the dots is not as easy as it may look, especially with large-scale datasets. Most ML libraries are not designed for distributed computation or they do not provide native support for pipeline creation and tuning. - [Databricks ML Pipelines](https://databricks.com/glossary/what-are-ml-pipelines)

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /databricks-datasets/adult/adult.data

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 1: Read in data from source and perform necessary pre-processing.

# COMMAND ----------

df = (spark
      .read
      .option("inferSchema", "True")
      .csv("/databricks-datasets/adult/adult.data")
     )

display(df)

# COMMAND ----------

# We can use a reduce operation with a lambda function to rename our columns as desired.

from functools import reduce

oldColumns = df.schema.names
newColumns = ["age", "workclass", "fnlwgt", "education", "education_num", "marital_status", "occupation", "relationship", "race", "sex", "capital_gain", "capital_loss", "hours_per_week", "native_country", "income"]

df = reduce(lambda df, idx: df.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), df)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### When creating a machine learning model, we must separate the categorical and numerical columns.  This is to allow for future processing steps such as indexing categorical column values.  We also need to remove the label (column we want to predict), from the list of columns.

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

# MAGIC %md
# MAGIC #### Step 2: Create the stages that will be added as steps in our pipeline.
# MAGIC 
# MAGIC Pipelines allow us to create multiple stages that must occur in order to get our datasets into a form to either train a model or generate a score.
# MAGIC 
# MAGIC Pipelines consist of `Estimators` and `Transformers`:
# MAGIC * Transformer: A feature transformer might take a DataFrame, read a column (e.g., text), map it into a new column (e.g., feature vectors), and output a new DataFrame with the mapped column 
# MAGIC * Estimator: A learning algorithm such as LogisticRegression is an Estimator

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

# MAGIC %md
# MAGIC 
# MAGIC #### When training, testing, or scoring a model, the input into the desired algorithm is in the form of a vector.  The vector assembler takes all of the column values for a single record and creates a vector (array of data), to feed into the desired algorithm.

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

# MAGIC %md
# MAGIC #### Step 3: Split our initial DataFrame into Test and Train DataFrames

# COMMAND ----------

(trainingData, testData) = preppedDataDF.randomSplit([0.7, 0.3], seed = 100)
print(trainingData.count())
print(testData.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Fit our model by using the training data
# MAGIC When we call the `.fit()` function, the pipeline stages are executed on the data in that dataset.

# COMMAND ----------

lrModel = LogisticRegression().fit(trainingData)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5: Run our test data through the fit model, and view the predicted results for model evaluation

# COMMAND ----------

predictionsDF = (lrModel.transform(testData)).select("income", "label", "prediction", "probability")

# COMMAND ----------

predictionsDF.registerTempTable("incomePredictionsOutputDF")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT 
# MAGIC   *
# MAGIC FROM incomePredictionsOutputDF

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

