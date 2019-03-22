# Databricks notebook source
import datetime
from pyspark.sql.functions import lit
import os

# COMMAND ----------

dbutils.fs.rm("/examples", True)

# COMMAND ----------

if os.path.exists("/examples/nfl-data/play-by-play-get") == True:
  print("Path Exists")
else:
  dbutils.fs.mkdirs("/examples/nfl-data/play-by-play-get")

# COMMAND ----------

# MAGIC %sh wget
# MAGIC 
# MAGIC for i in {2009..2019}
# MAGIC do
# MAGIC   wget https://raw.githubusercontent.com/ryurko/nflscrapR-data/master/play_by_play_data/regular_season/reg_pbp_"$i".csv -O reg_pbp_"$i".csv -O /tmp/reg-pbp-"$i".csv
# MAGIC done

# COMMAND ----------

years = list(range(2009,2019))

timestamp = str('{:%Y-%m-%d}'.format(datetime.datetime.now()))

for year in years:
  dbutils.fs.mv("file:///tmp/reg-pbp-"+str(year)+".csv", "/examples/nfl-data/play-by-play-get/"+timestamp+"/raw/pbp-"+str(year)+".csv")

# COMMAND ----------

# MAGIC %fs ls /examples/nfl-data/play-by-play-get/2019-03-01/raw-trans/

# COMMAND ----------

years = list(range(2009,2019))

timestamp = str('{:%Y-%m-%d}'.format(datetime.datetime.now()))
basePath = "/examples/nfl-data/play-by-play-get/"+timestamp+"/raw/pbp-"

for year in years:
  dataPath = basePath+str(year)+".csv"
  nflDF = spark.read.format("csv").option("header","true").option("inferSchema","true").load(dataPath)
  
  nflDF = nflDF.withColumn("season", lit(year))
  
  nflDF.write.format("csv").option("header","true").mode("overwrite").save("/examples/nfl-data/play-by-play-get/" + timestamp + "/raw-trans/pbp-" + str(year))

# COMMAND ----------

df = spark.read.option("header","True").option("inferSchema", "True").csv("/examples/nfl-data/play-by-play-get/"+timestamp+"/raw-trans/pbp-2012")

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType, TimestampType

nflSchema = StructType([
        StructField("play_id", IntegerType())
      , StructField("game_id", IntegerType())
      , StructField("home_team", StringType())
      , StructField("away_team", StringType())
      , StructField("posteam", StringType())
      , StructField("posteam_type", StringType())
      , StructField("defteam", StringType())
      , StructField("side_of_field", StringType())
      , StructField("yardline_100", StringType())
      , StructField("game_date", TimestampType())
      , StructField("quarter_seconds_remaining", StringType())
      , StructField("half_seconds_remaining", StringType())
      , StructField("game_seconds_remaining", StringType())
      , StructField("game_half", StringType())
      , StructField("quarter_end", IntegerType())
      , StructField("drive", IntegerType())
      , StructField("sp", IntegerType())
      , StructField("qtr", IntegerType())
      , StructField("down", StringType())
      , StructField("goal_to_go", StringType())
      , StructField("time", StringType())
      , StructField("yrdln", StringType())
      , StructField("ydstogo", IntegerType())
      , StructField("ydsnet", IntegerType())
      , StructField("desc", StringType())
      , StructField("play_type", StringType())
      , StructField("yards_gained", IntegerType())
      , StructField("shotgun", IntegerType())
      , StructField("no_huddle", IntegerType())
      , StructField("qb_dropback", StringType())
      , StructField("qb_kneel", IntegerType())
      , StructField("qb_spike", IntegerType())
      , StructField("qb_scramble", IntegerType())
      , StructField("pass_length", StringType())
      , StructField("pass_location", StringType())
      , StructField("air_yards", StringType())
      , StructField("yards_after_catch", StringType())
      , StructField("run_location", StringType())
      , StructField("run_gap", StringType())
      , StructField("field_goal_result", StringType())
      , StructField("kick_distance", StringType())
      , StructField("extra_point_result", StringType())
      , StructField("two_point_conv_result", StringType())
      , StructField("home_timeouts_remaining", IntegerType())
      , StructField("away_timeouts_remaining", IntegerType())
      , StructField("timeout", StringType())
      , StructField("timeout_team", StringType())
      , StructField("td_team", StringType())
      , StructField("posteam_timeouts_remaining", StringType())
      , StructField("defteam_timeouts_remaining", StringType())
      , StructField("total_home_score", IntegerType())
      , StructField("total_away_score", IntegerType())
      , StructField("posteam_score", StringType())
      , StructField("defteam_score", StringType())
      , StructField("score_differential", StringType())
      , StructField("posteam_score_post", StringType())
      , StructField("defteam_score_post", StringType())
      , StructField("score_differential_post", StringType())
      , StructField("no_score_prob", StringType())
      , StructField("opp_fg_prob", StringType())
      , StructField("opp_safety_prob", StringType())
      , StructField("opp_td_prob", StringType())
      , StructField("fg_prob", StringType())
      , StructField("safety_prob", StringType())
      , StructField("td_prob", StringType())
      , StructField("extra_point_prob", DoubleType())
      , StructField("two_point_conversion_prob", DoubleType())
      , StructField("ep", StringType())
      , StructField("epa", StringType())
      , StructField("total_home_epa", DoubleType())
      , StructField("total_away_epa", DoubleType())
      , StructField("total_home_rush_epa", DoubleType())
      , StructField("total_away_rush_epa", DoubleType())
      , StructField("total_home_pass_epa", DoubleType())
      , StructField("total_away_pass_epa", DoubleType())
      , StructField("air_epa", StringType())
      , StructField("yac_epa", StringType())
      , StructField("comp_air_epa", StringType())
      , StructField("comp_yac_epa", StringType())
      , StructField("total_home_comp_air_epa", DoubleType())
      , StructField("total_away_comp_air_epa", DoubleType())
      , StructField("total_home_comp_yac_epa", DoubleType())
      , StructField("total_away_comp_yac_epa", DoubleType())
      , StructField("total_home_raw_air_epa", DoubleType())
      , StructField("total_away_raw_air_epa", DoubleType())
      , StructField("total_home_raw_yac_epa", DoubleType())
      , StructField("total_away_raw_yac_epa", DoubleType())
      , StructField("wp", StringType())
      , StructField("def_wp", StringType())
      , StructField("home_wp", StringType())
      , StructField("away_wp", StringType())
      , StructField("wpa", StringType())
      , StructField("home_wp_post", StringType())
      , StructField("away_wp_post", StringType())
      , StructField("total_home_rush_wpa", DoubleType())
      , StructField("total_away_rush_wpa", DoubleType())
      , StructField("total_home_pass_wpa", DoubleType())
      , StructField("total_away_pass_wpa", DoubleType())
      , StructField("air_wpa", StringType())
      , StructField("yac_wpa", StringType())
      , StructField("comp_air_wpa", StringType())
      , StructField("comp_yac_wpa", StringType())
      , StructField("total_home_comp_air_wpa", DoubleType())
      , StructField("total_away_comp_air_wpa", DoubleType())
      , StructField("total_home_comp_yac_wpa", DoubleType())
      , StructField("total_away_comp_yac_wpa", DoubleType())
      , StructField("total_home_raw_air_wpa", DoubleType())
      , StructField("total_away_raw_air_wpa", DoubleType())
      , StructField("total_home_raw_yac_wpa", DoubleType())
      , StructField("total_away_raw_yac_wpa", DoubleType())
      , StructField("punt_blocked", StringType())
      , StructField("first_down_rush", StringType())
      , StructField("first_down_pass", StringType())
      , StructField("first_down_penalty", StringType())
      , StructField("third_down_converted", StringType())
      , StructField("third_down_failed", StringType())
      , StructField("fourth_down_converted", StringType())
      , StructField("fourth_down_failed", StringType())
      , StructField("incomplete_pass", StringType())
      , StructField("interception", StringType())
      , StructField("punt_inside_twenty", StringType())
      , StructField("punt_in_endzone", StringType())
      , StructField("punt_out_of_bounds", StringType())
      , StructField("punt_downed", StringType())
      , StructField("punt_fair_catch", StringType())
      , StructField("kickoff_inside_twenty", StringType())
      , StructField("kickoff_in_endzone", StringType())
      , StructField("kickoff_out_of_bounds", StringType())
      , StructField("kickoff_downed", StringType())
      , StructField("kickoff_fair_catch", StringType())
      , StructField("fumble_forced", StringType())
      , StructField("fumble_not_forced", StringType())
      , StructField("fumble_out_of_bounds", StringType())
      , StructField("solo_tackle", StringType())
      , StructField("safety", StringType())
      , StructField("penalty", StringType())
      , StructField("tackled_for_loss", StringType())
      , StructField("fumble_lost", StringType())
      , StructField("own_kickoff_recovery", StringType())
      , StructField("own_kickoff_recovery_td", StringType())
      , StructField("qb_hit", StringType())
      , StructField("rush_attempt", StringType())
      , StructField("pass_attempt", StringType())
      , StructField("sack", StringType())
      , StructField("touchdown", StringType())
      , StructField("pass_touchdown", StringType())
      , StructField("rush_touchdown", StringType())
      , StructField("return_touchdown", StringType())
      , StructField("extra_point_attempt", StringType())
      , StructField("two_point_attempt", StringType())
      , StructField("field_goal_attempt", StringType())
      , StructField("kickoff_attempt", StringType())
      , StructField("punt_attempt", StringType())
      , StructField("fumble", StringType())
      , StructField("complete_pass", StringType())
      , StructField("assist_tackle", StringType())
      , StructField("lateral_reception", StringType())
      , StructField("lateral_rush", StringType())
      , StructField("lateral_return", StringType())
      , StructField("lateral_recovery", StringType())
      , StructField("passer_player_id", StringType())
      , StructField("passer_player_name", StringType())
      , StructField("receiver_player_id", StringType())
      , StructField("receiver_player_name", StringType())
      , StructField("rusher_player_id", StringType())
      , StructField("rusher_player_name", StringType())
      , StructField("lateral_receiver_player_id", StringType())
      , StructField("lateral_receiver_player_name", StringType())
      , StructField("lateral_rusher_player_id", StringType())
      , StructField("lateral_rusher_player_name", StringType())
      , StructField("lateral_sack_player_id", StringType())
      , StructField("lateral_sack_player_name", StringType())
      , StructField("interception_player_id", StringType())
      , StructField("interception_player_name", StringType())
      , StructField("lateral_interception_player_id", StringType())
      , StructField("lateral_interception_player_name", StringType())
      , StructField("punt_returner_player_id", StringType())
      , StructField("punt_returner_player_name", StringType())
      , StructField("lateral_punt_returner_player_id", StringType())
      , StructField("lateral_punt_returner_player_name", StringType())
      , StructField("kickoff_returner_player_name", StringType())
      , StructField("kickoff_returner_player_id", StringType())
      , StructField("lateral_kickoff_returner_player_id", StringType())
      , StructField("lateral_kickoff_returner_player_name", StringType())
      , StructField("punter_player_id", StringType())
      , StructField("punter_player_name", StringType())
      , StructField("kicker_player_name", StringType())
      , StructField("kicker_player_id", StringType())
      , StructField("own_kickoff_recovery_player_id", StringType())
      , StructField("own_kickoff_recovery_player_name", StringType())
      , StructField("blocked_player_id", StringType())
      , StructField("blocked_player_name", StringType())
      , StructField("tackle_for_loss_1_player_id", StringType())
      , StructField("tackle_for_loss_1_player_name", StringType())
      , StructField("tackle_for_loss_2_player_id", StringType())
      , StructField("tackle_for_loss_2_player_name", StringType())
      , StructField("qb_hit_1_player_id", StringType())
      , StructField("qb_hit_1_player_name", StringType())
      , StructField("qb_hit_2_player_id", StringType())
      , StructField("qb_hit_2_player_name", StringType())
      , StructField("forced_fumble_player_1_team", StringType())
      , StructField("forced_fumble_player_1_player_id", StringType())
      , StructField("forced_fumble_player_1_player_name", StringType())
      , StructField("forced_fumble_player_2_team", StringType())
      , StructField("forced_fumble_player_2_player_id", StringType())
      , StructField("forced_fumble_player_2_player_name", StringType())
      , StructField("solo_tackle_1_team", StringType())
      , StructField("solo_tackle_2_team", StringType())
      , StructField("solo_tackle_1_player_id", StringType())
      , StructField("solo_tackle_2_player_id", StringType())
      , StructField("solo_tackle_1_player_name", StringType())
      , StructField("solo_tackle_2_player_name", StringType())
      , StructField("assist_tackle_1_player_id", StringType())
      , StructField("assist_tackle_1_player_name", StringType())
      , StructField("assist_tackle_1_team", StringType())
      , StructField("assist_tackle_2_player_id", StringType())
      , StructField("assist_tackle_2_player_name", StringType())
      , StructField("assist_tackle_2_team", StringType())
      , StructField("assist_tackle_3_player_id", StringType())
      , StructField("assist_tackle_3_player_name", StringType())
      , StructField("assist_tackle_3_team", StringType())
      , StructField("assist_tackle_4_player_id", StringType())
      , StructField("assist_tackle_4_player_name", StringType())
      , StructField("assist_tackle_4_team", StringType())
      , StructField("pass_defense_1_player_id", StringType())
      , StructField("pass_defense_1_player_name", StringType())
      , StructField("pass_defense_2_player_id", StringType())
      , StructField("pass_defense_2_player_name", StringType())
      , StructField("fumbled_1_team", StringType())
      , StructField("fumbled_1_player_id", StringType())
      , StructField("fumbled_1_player_name", StringType())
      , StructField("fumbled_2_player_id", StringType())
      , StructField("fumbled_2_player_name", StringType())
      , StructField("fumbled_2_team", StringType())
      , StructField("fumble_recovery_1_team", StringType())
      , StructField("fumble_recovery_1_yards", StringType())
      , StructField("fumble_recovery_1_player_id", StringType())
      , StructField("fumble_recovery_1_player_name", StringType())
      , StructField("fumble_recovery_2_team", StringType())
      , StructField("fumble_recovery_2_yards", StringType())
      , StructField("fumble_recovery_2_player_id", StringType())
      , StructField("fumble_recovery_2_player_name", StringType())
      , StructField("return_team", StringType())
      , StructField("return_yards", IntegerType())
      , StructField("penalty_team", StringType())
      , StructField("penalty_player_id", StringType())
      , StructField("penalty_player_name", StringType())
      , StructField("penalty_yards", StringType())
      , StructField("replay_or_challenge", IntegerType())
      , StructField("replay_or_challenge_result", StringType())
      , StructField("penalty_type", StringType())
      , StructField("defensive_two_point_attempt", StringType())
      , StructField("defensive_two_point_conv", StringType())
      , StructField("defensive_extra_point_attempt", StringType())
      , StructField("defensive_extra_point_conv", StringType())
      , StructField("season", IntegerType())
])

# COMMAND ----------

basePath = ("/examples/nfl-data/play-by-play-get/")

years = list(range(2009,2019))
timestamp = str('{:%Y-%m-%d}'.format(datetime.datetime.now()))

appendedDF = 0

for year in years:
  fullPath = basePath + timestamp + '/raw-trans/pbp-'+str(year)+'/*.csv'
  if appendedDF == 0:
    df = spark.read.format("csv").option("header","true").schema(nflSchema).load(fullPath)
    appendedDF = df
  else:
    df = spark.read.format("csv").option("header","true").schema(nflSchema).load(fullPath)
  appendedDF = appendedDF.union(df)
  
appendedDF.registerTempTable("nflPBP")

appendedDF.write.format("parquet").option("header","true").mode("overwrite").save("/examples/nfl-data/play-by-play-get/" + timestamp + "/raw-nflFull")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT season FROM nflPBP

# COMMAND ----------

# MAGIC %fs ls examples/nfl-data/play-by-play-get/2019-03-01/raw-nflFull

# COMMAND ----------

nflFullDF = (spark
             .read
             .schema(nflSchema)
             .option("header", "True")
             .parquet("/examples/nfl-data/play-by-play-get/"+timestamp+"/raw-nflFull/")
            )

nflFullDF.cache()

display(nflFullDF)

# COMMAND ----------

from pyspark.sql.types import IntegerType

def yardsGainedWeight(yardsGained):
  if yardsGained > 0 and yardsGained <= 5:
    yardsGainedWeight = 1
  elif yardsGained > 5 and yardsGained <= 10:
    yardsGainedWeight = 2
  elif yardsGained > 10 and yardsGained <= 25:
    yardsGainedWeight = 3
  elif yardsGained > 25 and yardsGained <= 50:
    yardsGainedWeight = 4
  elif yardsGained > 50:
    yardsGainedWeight = 5
  else:
    yardsGainedWeight = 0
  
  return(yardsGainedWeight)

yardsGainedWeightUDF = spark.udf.register("yardsGainedWeightSQLUDF", yardsGainedWeight, IntegerType())

# COMMAND ----------

from pyspark.sql.types import IntegerType

def uniquePlayerComboID(passer_name, rusher_name, receiver_name):
  concat_players = str(passer_name) + str(rusher_name) + str(receiver_name)
  hash_players = hash(concat_players)
  
  return(hash_players)

uniquePlayerUDF = spark.udf.register("uniquePlayerSQLUDF", uniquePlayerComboID, IntegerType())

# COMMAND ----------

from pyspark.sql.types import IntegerType

def touchdownWeight(touchdown):
  if touchdown == '1':
    touchdownWeight = 5
  else:
    touchdownWeight = 0
    
  return(touchdownWeight)

touchdownWeightUDF = spark.udf.register("touchdownWeightSQLUDF", touchdownWeight, IntegerType())

# COMMAND ----------

from pyspark.sql.types import DoubleType

def avgPlayWeight(yardsGainedWeight, touchdownWeight):
  avgPlayWeight = (yardsGainedWeight + touchdownWeight)/2
  
  return(avgPlayWeight)

avgPlayWeightUDF = spark.udf.register("avgPlaySQLUDF", avgPlayWeight, DoubleType())

# COMMAND ----------

from pyspark.sql.types import IntegerType

def uniquePlayTypeID(play_type, def_team, yardline_100, qtr, down, pass_length, pass_location, run_location, run_gap, season):
  if play_type == 'run':
    concat_play = def_team + str(yardline_100) + str(qtr) + str(down) + run_location + run_gap + str(season)
    hash_play = hash(concat_play)
  elif play_type == 'pass':
    concat_play = def_team + str(yardline_100) + str(qtr) + str(down) + pass_length + pass_location +str(season)
    hash_play = hash(concat_play)
  else:
    None
  
  return(hash_play)

uniquePlayTypeUDF = spark.udf.register("uniquePlayTypeSQLUDF", uniquePlayTypeID, IntegerType())

# COMMAND ----------

from pyspark.sql.functions import col, avg, count, monotonically_increasing_id
from pyspark.sql.types import IntegerType

nflDF = (nflFullDF
                .select(
                  "play_id"
                  ,"game_id"
                  ,"home_team"
                  ,"away_team"
                  ,"posteam"
                  ,"posteam_type"
                  ,"defteam"
                  ,"side_of_field"
                  ,"yardline_100"
                  ,"game_date"
                  ,"quarter_seconds_remaining"
                  ,"half_seconds_remaining"
                  ,"game_seconds_remaining"
                  ,"game_half"
                  ,"quarter_end"
                  ,"drive"
                  ,"sp"
                  ,"qtr"
                  ,"down"
                  ,"goal_to_go"
                  ,"time"
                  ,"yrdln"
                  ,"ydstogo"
                  ,"ydsnet"
                  ,"desc"
                  ,"play_type"
                  ,"yards_gained"
                  ,"pass_length"
                  ,"pass_location"
                  ,"air_yards"
                  ,"yards_after_catch"
                  ,"run_location"
                  ,"run_gap"
                  ,"total_home_score"
                  ,"total_away_score"
                  ,"posteam_score"
                  ,"defteam_score"
                  ,"score_differential"
                  ,"incomplete_pass"
                  ,"interception"
                  ,"safety"
                  ,"tackled_for_loss"
                  ,"qb_hit"
                  ,"rush_attempt"
                  ,"pass_attempt"
                  ,"sack"
                  ,"touchdown"
                  ,"pass_touchdown"
                  ,"rush_touchdown"
                  ,"fumble"
                  ,"complete_pass"
                  ,"passer_player_id"
                  ,"passer_player_name"
                  ,"receiver_player_id"
                  ,"receiver_player_name"
                  ,"rusher_player_id"
                  ,"rusher_player_name"
                  ,"season")
                .where("play_type == 'run' or play_type == 'pass'")
                .withColumn("yardsGainedWeight", yardsGainedWeightUDF("yards_gained"))
                .withColumn("uniquePlayTypeID", uniquePlayTypeUDF("play_type", "defteam", "yardline_100", "qtr", "down", "pass_length", "pass_location", "run_location", "run_gap", "season"))
                .withColumn("uniquePlayerID", uniquePlayerUDF("passer_player_name", "rusher_player_name", "receiver_player_name"))
                .withColumn("touchdownWeight", touchdownWeightUDF("touchdown"))
                .withColumn("overallWeight", avgPlayWeightUDF("yardsGainedWeight","touchdownWeight"))
               )

nflReducedDF = (nflDF
                .select("*")
                .where("defteam <> 'CHI' or posteam <> 'CHI'")
               )

nflStreamDF = (nflDF
               .select("*")
               .where("posteam == 'CHI'")
              )

playerHashDF = (nflReducedDF
                .select("uniquePlayerID"
                        ,"passer_player_name"
                        ,"receiver_player_name"
                        ,"rusher_player_name")
                .groupBy("uniquePlayerID", "passer_player_name", "receiver_player_name", "rusher_player_name").agg(count("*"))
               ).drop("count(1)")

playTypeHashDF = (nflReducedDF
                  .select("uniquePlayTypeID"
                          ,"play_type"
                          ,"defteam"
                          ,"yardline_100"
                          ,"qtr"
                          ,"down"
                          ,"pass_length"
                          ,"pass_location"
                          ,"run_location"
                          ,"run_gap")
                  .groupBy("uniquePlayTypeID", "play_type", "defteam", "yardline_100", "qtr", "down", "pass_length", "pass_location", "run_location", "run_gap").agg(count("*"))
                 ).drop("count(1)")


nflDF.cache()
nflReducedDF.cache()
playerHashDF.cache()
playTypeHashDF.cache()

#nflDF.show(5)
#nflReducedDF.show(5)
#playerHashDF.show(5)
#playTypeHashDF.show(5)

# COMMAND ----------

nflStreamPath = "/examples/nfl-data/play-by-play-get/"+timestamp+"/nflStream/"

dbutils.fs.mkdirs(nflStreamPath)

nflStreamDF.write.format("parquet").mode("overwrite").save(nflStreamPath)

# COMMAND ----------

seed = 100
(trainDF, testDF, validationDF) = nflReducedDF.randomSplit(weights = [0.6, 0.2, 0.2], seed = seed)

trainDF.cache()
testDF.cache()
validationDF.cache()

print('Training DataFrame: {0}\n Test DataFrame: {1}\n Validation DataFrame: {2}'.format(trainDF.count(), testDF.count(), validationDF.count()))

# COMMAND ----------

from pyspark.ml.recommendation import ALS

# Let's initialize our ALS learner
als = ALS()
"""
class pyspark.ml.recommendation.ALS(self, rank=10, maxIter=10, regParam=0.1, numUserBlocks=10, numItemBlocks=10, implicitPrefs=false, alpha=1.0, userCol="user", itemCol="item", seed=None, ratingCol="rating", nonnegative=false, checkpointInterval=10)[source]
# Now we set the parameters for the method
"""
als.setMaxIter(5).setSeed(seed).setRegParam(0.1).setItemCol("uniquePlayTypeID").setUserCol("uniquePlayerID").setRatingCol("overallWeight")

# Now let's compute an evaluation metric for our test dataset
from pyspark.ml.evaluation import RegressionEvaluator

# Create an RMSE evaluator using the label and predicted columns
reg_eval = RegressionEvaluator(predictionCol="prediction", labelCol="overallWeight", metricName="rmse")

tolerance = 0.03
ranks = [4, 8, 12]
errors = [0, 0, 0]
models = [0, 0, 0]
err = 0
min_error = float('inf')
best_rank = -1
for rank in ranks:
  als.setRank(rank)
  # Create the model with these parameters.
  model = als.fit(trainDF)
  predict_df = model.transform(validationDF)

  # Remove NaN values from prediction (due to SPARK-14489)
  predicted_weight_df = predict_df.filter(predict_df.prediction != float('nan'))

  # Run the previously created RMSE evaluator, reg_eval, on the predicted_ratings_df DataFrame
  #error = reg_eval.<FILL_IN>
  error = reg_eval.evaluate(predicted_weight_df)
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

chiDF = (nflReducedDF
                .select("yards_gained"
                        ,"passer_player_name"
                        ,"rusher_player_name"
                        ,"receiver_player_name"
                        ,"touchdown"
                        ,"play_type"
                        ,"posteam"
                        ,"defteam"
                        ,"yardline_100"
                        ,"qtr"
                        ,"down"
                        ,"pass_length"
                        ,"pass_location"
                        ,"run_location"
                        ,"run_gap"
                        ,"season"
                        ,"uniquePlayerID"
                        ,"uniquePlayTypeID"
                        ,"overallWeight")
                .where("posteam == 'CHI'")
               )

reducedChiDF = (chiDF
                .select("uniquePlayerID", "uniquePlayTypeID", "overallWeight")
               )

# COMMAND ----------

raw_predicted_ratings_df = (my_model
                        .transform(reducedChiDF)
                       )

predicted_ratings_df = raw_predicted_ratings_df.filter(raw_predicted_ratings_df.prediction != float('nan'))

# COMMAND ----------

predictedPlayTypeDF = predicted_ratings_df.join(playTypeHashDF, "uniquePlayTypeID")

display(predictedPlayTypeDF)

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, LongType, StringType, TimestampType, DoubleType

nflStreamSchema = StructType([
StructField("play_id", IntegerType())
, StructField("game_id", IntegerType())
, StructField("home_team", StringType())
, StructField("away_team", StringType())
, StructField("posteam", StringType())
, StructField("posteam_type", StringType())
, StructField("defteam", StringType())
, StructField("side_of_field", StringType())
, StructField("yardline_100", StringType())
, StructField("game_date", TimestampType())
, StructField("quarter_seconds_remaining", StringType())
, StructField("half_seconds_remaining", StringType())
, StructField("game_seconds_remaining", StringType())
, StructField("game_half", StringType())
, StructField("quarter_end", IntegerType())
, StructField("drive", IntegerType())
, StructField("sp", IntegerType())
, StructField("qtr", IntegerType())
, StructField("down", StringType())
, StructField("goal_to_go", StringType())
, StructField("time", StringType())
, StructField("yrdln", StringType())
, StructField("ydstogo", IntegerType())
, StructField("ydsnet", IntegerType())
, StructField("desc", StringType())
, StructField("play_type", StringType())
, StructField("yards_gained", IntegerType())
, StructField("pass_length", StringType())
, StructField("pass_location", StringType())
, StructField("air_yards", StringType())
, StructField("yards_after_catch", StringType())
, StructField("run_location", StringType())
, StructField("run_gap", StringType())
, StructField("total_home_score", IntegerType())
, StructField("total_away_score", IntegerType())
, StructField("posteam_score", StringType())
, StructField("defteam_score", StringType())
, StructField("score_differential", StringType())
, StructField("incomplete_pass", StringType())
, StructField("interception", StringType())
, StructField("safety", StringType())
, StructField("tackled_for_loss", StringType())
, StructField("qb_hit", StringType())
, StructField("rush_attempt", StringType())
, StructField("pass_attempt", StringType())
, StructField("sack", StringType())
, StructField("touchdown", StringType())
, StructField("pass_touchdown", StringType())
, StructField("rush_touchdown", StringType())
, StructField("fumble", StringType())
, StructField("complete_pass", StringType())
, StructField("passer_player_id", StringType())
, StructField("passer_player_name", StringType())
, StructField("receiver_player_id", StringType())
, StructField("receiver_player_name", StringType())
, StructField("rusher_player_id", StringType())
, StructField("rusher_player_name", StringType())
, StructField("season", IntegerType())
, StructField("yardsGainedWeight", IntegerType())
, StructField("uniquePlayTypeID", IntegerType())
, StructField("uniquePlayerID", IntegerType())
, StructField("touchdownWeight", IntegerType())
, StructField("overallWeight", DoubleType())
])

# COMMAND ----------

nflCheckpointPath = "/nflStreamCheck"

dbutils.fs.mkdirs(nflCheckpointPath)

nflStreamPath = "/examples/nfl-data/play-by-play-get/"+timestamp+"/nflStream/"

chiStreamDF = (spark
               .readStream
               .schema(nflStreamSchema)
               .option("maxFilesPerTrigger", 1)
               .option("checkpointLocation", nflCheckpointPath)
               .format("parquet")
               .load(nflStreamPath)
              )

# COMMAND ----------

rawChiPredStreamDF = (my_model
                   .transform(chiStreamDF)
                  ).select("uniquePlayerID", "uniquePlayTypeID", "prediction")

chiPredStreamDF = rawChiPredStreamDF.filter(rawChiPredStreamDF.prediction != float('nan'))

# COMMAND ----------

predictPlayTypeStreamDF = (chiPredStreamDF
                       .join(playTypeHashDF, "uniquePlayTypeID")
                      ).select("prediction"
                               ,"play_type"
                               ,"defteam"
                               ,"yardline_100"
                               ,"qtr"
                               ,"down"
                               ,"pass_length"
                               ,"pass_location"
                               ,"run_location"
                               ,"run_gap"
                              )

# COMMAND ----------

playPredictionPath = "/examples/nfl-data/play-by-play-get/"+timestamp+"/nflStream/playPrediction"

dbutils.fs.mkdirs(playPredictionPath)

(predictPlayTypeStreamDF
 .writeStream
 .format("delta")
 .partitionBy("defteam")
 .option("checkpointLocation", nflCheckpointPath)
 .option("path", playPredictionPath)
 .outputMode("append")
 .start()
)

# COMMAND ----------

# MAGIC %fs ls /examples/nfl-data/play-by-play-get/2019-03-01/nflStream/playPrediction

# COMMAND ----------

spark.sql("""
  DROP TABLE IF EXISTS play_recommendations
""")
spark.sql("""
  CREATE TABLE play_recommendations
  USING DELTA 
  LOCATION '{}' 
""".format(playPredictionPath))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM play_recommendations ORDER BY prediction DESC

# COMMAND ----------

[q.stop() for q in spark.streams.active]

# COMMAND ----------

