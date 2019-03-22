# Databricks notebook source
# MAGIC %fs ls /databricks-datasets/cs110x/ml-20m/data-001

# COMMAND ----------

# MAGIC %fs
# MAGIC head /databricks-datasets/cs110x/ml-20m/data-001/movies.csv

# COMMAND ----------

# MAGIC %fs
# MAGIC head /databricks-datasets/cs110x/ml-20m/data-001/ratings.csv

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

movieBasePath = '/databricks-datasets/cs110x/ml-20m/data-001'
movieRatingsFile = '/ratings.csv'
movieListFile = '/movies.csv'

ratingSchema = StructType([
  StructField("userID", IntegerType())
  , StructField("movieID", IntegerType())
  , StructField("rating", DoubleType())
  , StructField("timestamp", DoubleType())
])

movieSchema = StructType([
  StructField("movieID", IntegerType())
  , StructField("title", StringType())
  , StructField("genre", StringType())
])

movieDF = (spark
           .read
           .option("header", "True")
           .schema(movieSchema)
           .csv(movieBasePath + movieListFile)
          )

ratingDF = (spark
            .read
            .option("header", "True")
            .schema(ratingSchema)
            .csv(movieBasePath + movieRatingsFile)
           )

# COMMAND ----------

display(movieDF)

# COMMAND ----------

display(ratingDF)

# COMMAND ----------

movieDF.cache()
ratingDF.cache()

movieDFCount = movieDF.count()
ratingDFCount = ratingDF.count()

print("There are {0} movies and {1} ratings in the datasets".format(movieDFCount, ratingDFCount))

# COMMAND ----------

from pyspark.sql.functions import avg, desc, count

top20RatingsDF = (ratingDF
                 .groupBy("movieID").agg(count("userID").alias("userCount"),avg("rating").alias("avgRating"))
                 ).sort(desc("userCount"),desc("avgRating")).limit(20)

top20MoviesDF = top20RatingsDF.join(movieDF, top20RatingsDF.movieID == movieDF.movieID, 'inner')

display(top20MoviesDF.sort(desc("avgRating")))

# COMMAND ----------

seed = 1800009193

(trainDF, testDF, validationDF) = ratingDF.randomSplit([0.6, 0.2, 0.2], seed = seed)

trainDF.cache()
testDF.cache()
validationDF.cache()

print("Training: {0} \nTest: {1} \nValidation: {2}".format(trainDF.count(), testDF.count(), validationDF.count()))

# COMMAND ----------

from pyspark.ml.recommendation import ALS

als = ALS()

"""
class pyspark.ml.recommendation.ALS(self, rank=10, maxIter=10, regParam=0.1, numUserBlocks=10, numItemBlocks=10, implicitPrefs=false, alpha=1.0, userCol="user", itemCol="item", seed=None, ratingCol="rating", nonnegative=false, checkpointInterval=10)[source]
# Now we set the parameters for the method
"""

als.setMaxIter(5).setSeed(seed).setRegParam(0.1).setItemCol("movieID").setUserCol("userID").setRatingCol("rating")
  
from pyspark.ml.evaluation import RegressionEvaluator

reg_eval = RegressionEvaluator(predictionCol = "prediction", labelCol = "rating", metricName = "rmse")

tolerance = 0.03
ranks = [4, 8, 12]
errors = [0, 0, 0]
models = [0, 0, 0]
err = 0
min_error = float('inf')
best_rank = -1
for rank in ranks:
  als.setRank(rank)
  model = als.fit(trainDF)
  predictDF = model.transform(validationDF)
  
  predicted_ratings_df = predictDF.filter(predictDF.prediction != float('nan'))
  
  error = reg_eval.evaluate(predicted_ratings_df)
  errors[err] = error
  models[err] = model
  print('For rank {0} the RMSE is {1}'.format(rank, error))
  if error < min_error:
    min_error = error
    best_rank = err
  err += 1
  
als.setRank(ranks[best_rank])
print('The best model was trained with rank {0}'.format(ranks[best_rank]))
my_model = models[best_rank]

# COMMAND ----------

predictDF = my_model.transform(testDF)

predictedTestDF = predictDF.filter(predictDF.prediction != float('nan'))

test_RMSE = reg_eval.evaluate(predictedTestDF)

print('The model had a RMSE on the test set of {0}'.format(test_RMSE))

# COMMAND ----------

