# Databricks notebook source
import pandas as pd
import glob
import numpy as np
from pyspark.sql.types import StructType

# COMMAND ----------

basePath = ("/mnt/nfl-data/pbp-get/test/")

years = list(range(2009,2019))

appendedDF = 0

for year in years:
  fullPath = basePath + 'pbp-'+str(year)+'/*.csv'
  if appendedDF == 0:
    df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(fullPath)
    appendedDF = df
  else:
    df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(fullPath)
  appendedDF = appendedDF.union(df)
  
appendedDF.registerTempTable("nflPBP")

appendedDF.write.format("csv").option("header","true").save("/mnt/nfl-data/pbp-get/test/nflFull")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE nfl;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS nflFull;
# MAGIC 
# MAGIC CREATE TABLE nflFull AS
# MAGIC 
# MAGIC SELECT
# MAGIC   a.play_id
# MAGIC   ,a.game_id
# MAGIC   ,B1.CorrectTeamAbr AS home_team_abr
# MAGIC   ,B1.FullTeamName AS home_team_full_nm
# MAGIC   ,B2.CorrectTeamAbr AS away_team_abr
# MAGIC   ,B2.FullTeamName AS away_team_full_nm
# MAGIC   ,B3.CorrectTeamAbr AS pos_team_abr
# MAGIC   ,B3.FullTeamName AS pos_team_full_nm
# MAGIC   ,B4.CorrectTeamAbr AS def_team_abr
# MAGIC   ,B4.FullTeamName AS def_team_full_nm
# MAGIC   ,B5.CorrectTeamAbr AS side_of_field_abr
# MAGIC   ,B5.FullTeamName AS side_of_field_full_nm
# MAGIC   ,a.yardline_100
# MAGIC   ,a.game_date
# MAGIC   ,a.quarter_seconds_remaining
# MAGIC   ,a.half_seconds_remaining
# MAGIC   ,a.game_seconds_remaining
# MAGIC   ,a.game_half
# MAGIC   ,a.quarter_end
# MAGIC   ,a.drive
# MAGIC   ,a.sp
# MAGIC   ,a.qtr
# MAGIC   ,a.down
# MAGIC   ,a.goal_to_go
# MAGIC   ,a.time
# MAGIC   ,a.yrdln
# MAGIC   ,a.ydstogo
# MAGIC   ,a.ydsnet
# MAGIC   ,a.desc
# MAGIC   ,a.play_type
# MAGIC   ,a.yards_gained
# MAGIC   ,a.shotgun
# MAGIC   ,a.no_huddle
# MAGIC   ,a.qb_dropback
# MAGIC   ,a.qb_kneel
# MAGIC   ,a.qb_spike
# MAGIC   ,a.qb_scramble
# MAGIC   ,a.pass_length
# MAGIC   ,a.pass_location
# MAGIC   ,a.air_yards
# MAGIC   ,a.yards_after_catch
# MAGIC   ,a.run_location
# MAGIC   ,a.run_gap
# MAGIC   ,a.field_goal_result
# MAGIC   ,a.kick_distance
# MAGIC   ,a.extra_point_result
# MAGIC   ,a.two_point_conv_result
# MAGIC   ,a.home_timeouts_remaining
# MAGIC   ,a.away_timeouts_remaining
# MAGIC   ,a.timeout
# MAGIC   ,a.timeout_team
# MAGIC   ,a.td_team
# MAGIC   ,a.posteam_timeouts_remaining
# MAGIC   ,a.defteam_timeouts_remaining
# MAGIC   ,a.total_home_score
# MAGIC   ,a.total_away_score
# MAGIC   ,a.posteam_score
# MAGIC   ,a.defteam_score
# MAGIC   ,a.score_differential
# MAGIC   ,a.posteam_score_post
# MAGIC   ,a.defteam_score_post
# MAGIC   ,a.score_differential_post
# MAGIC   ,a.no_score_prob
# MAGIC   ,a.opp_fg_prob
# MAGIC   ,a.opp_safety_prob
# MAGIC   ,a.opp_td_prob
# MAGIC   ,a.fg_prob
# MAGIC   ,a.safety_prob
# MAGIC   ,a.td_prob
# MAGIC   ,a.extra_point_prob
# MAGIC   ,a.two_point_conversion_prob
# MAGIC   ,a.ep
# MAGIC   ,a.epa
# MAGIC   ,a.total_home_epa
# MAGIC   ,a.total_away_epa
# MAGIC   ,a.total_home_rush_epa
# MAGIC   ,a.total_away_rush_epa
# MAGIC   ,a.total_home_pass_epa
# MAGIC   ,a.total_away_pass_epa
# MAGIC   ,a.air_epa
# MAGIC   ,a.yac_epa
# MAGIC   ,a.comp_air_epa
# MAGIC   ,a.comp_yac_epa
# MAGIC   ,a.total_home_comp_air_epa
# MAGIC   ,a.total_away_comp_air_epa
# MAGIC   ,a.total_home_comp_yac_epa
# MAGIC   ,a.total_away_comp_yac_epa
# MAGIC   ,a.total_home_raw_air_epa
# MAGIC   ,a.total_away_raw_air_epa
# MAGIC   ,a.total_home_raw_yac_epa
# MAGIC   ,a.total_away_raw_yac_epa
# MAGIC   ,a.wp
# MAGIC   ,a.def_wp
# MAGIC   ,a.home_wp
# MAGIC   ,a.away_wp
# MAGIC   ,a.wpa
# MAGIC   ,a.home_wp_post
# MAGIC   ,a.away_wp_post
# MAGIC   ,a.total_home_rush_wpa
# MAGIC   ,a.total_away_rush_wpa
# MAGIC   ,a.total_home_pass_wpa
# MAGIC   ,a.total_away_pass_wpa
# MAGIC   ,a.air_wpa
# MAGIC   ,a.yac_wpa
# MAGIC   ,a.comp_air_wpa
# MAGIC   ,a.comp_yac_wpa
# MAGIC   ,a.total_home_comp_air_wpa
# MAGIC   ,a.total_away_comp_air_wpa
# MAGIC   ,a.total_home_comp_yac_wpa
# MAGIC   ,a.total_away_comp_yac_wpa
# MAGIC   ,a.total_home_raw_air_wpa
# MAGIC   ,a.total_away_raw_air_wpa
# MAGIC   ,a.total_home_raw_yac_wpa
# MAGIC   ,a.total_away_raw_yac_wpa
# MAGIC   ,a.punt_blocked
# MAGIC   ,a.first_down_rush
# MAGIC   ,a.first_down_pass
# MAGIC   ,a.first_down_penalty
# MAGIC   ,a.third_down_converted
# MAGIC   ,a.third_down_failed
# MAGIC   ,a.fourth_down_converted
# MAGIC   ,a.fourth_down_failed
# MAGIC   ,a.incomplete_pass
# MAGIC   ,a.interception
# MAGIC   ,a.punt_inside_twenty
# MAGIC   ,a.punt_in_endzone
# MAGIC   ,a.punt_out_of_bounds
# MAGIC   ,a.punt_downed
# MAGIC   ,a.punt_fair_catch
# MAGIC   ,a.kickoff_inside_twenty
# MAGIC   ,a.kickoff_in_endzone
# MAGIC   ,a.kickoff_out_of_bounds
# MAGIC   ,a.kickoff_downed
# MAGIC   ,a.kickoff_fair_catch
# MAGIC   ,a.fumble_forced
# MAGIC   ,a.fumble_not_forced
# MAGIC   ,a.fumble_out_of_bounds
# MAGIC   ,a.solo_tackle
# MAGIC   ,a.safety
# MAGIC   ,a.penalty
# MAGIC   ,a.tackled_for_loss
# MAGIC   ,a.fumble_lost
# MAGIC   ,a.own_kickoff_recovery
# MAGIC   ,a.own_kickoff_recovery_td
# MAGIC   ,a.qb_hit
# MAGIC   ,a.rush_attempt
# MAGIC   ,a.pass_attempt
# MAGIC   ,a.sack
# MAGIC   ,a.touchdown
# MAGIC   ,a.pass_touchdown
# MAGIC   ,a.rush_touchdown
# MAGIC   ,a.return_touchdown
# MAGIC   ,a.extra_point_attempt
# MAGIC   ,a.two_point_attempt
# MAGIC   ,a.field_goal_attempt
# MAGIC   ,a.kickoff_attempt
# MAGIC   ,a.punt_attempt
# MAGIC   ,a.fumble
# MAGIC   ,a.complete_pass
# MAGIC   ,a.assist_tackle
# MAGIC   ,a.lateral_reception
# MAGIC   ,a.lateral_rush
# MAGIC   ,a.lateral_return
# MAGIC   ,a.lateral_recovery
# MAGIC   ,a.passer_player_id
# MAGIC   ,a.passer_player_name
# MAGIC   ,a.receiver_player_id
# MAGIC   ,a.receiver_player_name
# MAGIC   ,a.rusher_player_id
# MAGIC   ,a.rusher_player_name
# MAGIC   ,a.lateral_receiver_player_id
# MAGIC   ,a.lateral_receiver_player_name
# MAGIC   ,a.lateral_rusher_player_id
# MAGIC   ,a.lateral_rusher_player_name
# MAGIC   ,a.lateral_sack_player_id
# MAGIC   ,a.lateral_sack_player_name
# MAGIC   ,a.interception_player_id
# MAGIC   ,a.interception_player_name
# MAGIC   ,a.lateral_interception_player_id
# MAGIC   ,a.lateral_interception_player_name
# MAGIC   ,a.punt_returner_player_id
# MAGIC   ,a.punt_returner_player_name
# MAGIC   ,a.lateral_punt_returner_player_id
# MAGIC   ,a.lateral_punt_returner_player_name
# MAGIC   ,a.kickoff_returner_player_name
# MAGIC   ,a.kickoff_returner_player_id
# MAGIC   ,a.lateral_kickoff_returner_player_id
# MAGIC   ,a.lateral_kickoff_returner_player_name
# MAGIC   ,a.punter_player_id
# MAGIC   ,a.punter_player_name
# MAGIC   ,a.kicker_player_name
# MAGIC   ,a.kicker_player_id
# MAGIC   ,a.own_kickoff_recovery_player_id
# MAGIC   ,a.own_kickoff_recovery_player_name
# MAGIC   ,a.blocked_player_id
# MAGIC   ,a.blocked_player_name
# MAGIC   ,a.tackle_for_loss_1_player_id
# MAGIC   ,a.tackle_for_loss_1_player_name
# MAGIC   ,a.tackle_for_loss_2_player_id
# MAGIC   ,a.tackle_for_loss_2_player_name
# MAGIC   ,a.qb_hit_1_player_id
# MAGIC   ,a.qb_hit_1_player_name
# MAGIC   ,a.qb_hit_2_player_id
# MAGIC   ,a.qb_hit_2_player_name
# MAGIC   ,a.forced_fumble_player_1_team
# MAGIC   ,a.forced_fumble_player_1_player_id
# MAGIC   ,a.forced_fumble_player_1_player_name
# MAGIC   ,a.forced_fumble_player_2_team
# MAGIC   ,a.forced_fumble_player_2_player_id
# MAGIC   ,a.forced_fumble_player_2_player_name
# MAGIC   ,a.solo_tackle_1_team
# MAGIC   ,a.solo_tackle_2_team
# MAGIC   ,a.solo_tackle_1_player_id
# MAGIC   ,a.solo_tackle_2_player_id
# MAGIC   ,a.solo_tackle_1_player_name
# MAGIC   ,a.solo_tackle_2_player_name
# MAGIC   ,a.assist_tackle_1_player_id
# MAGIC   ,a.assist_tackle_1_player_name
# MAGIC   ,a.assist_tackle_1_team
# MAGIC   ,a.assist_tackle_2_player_id
# MAGIC   ,a.assist_tackle_2_player_name
# MAGIC   ,a.assist_tackle_2_team
# MAGIC   ,a.assist_tackle_3_player_id
# MAGIC   ,a.assist_tackle_3_player_name
# MAGIC   ,a.assist_tackle_3_team
# MAGIC   ,a.assist_tackle_4_player_id
# MAGIC   ,a.assist_tackle_4_player_name
# MAGIC   ,a.assist_tackle_4_team
# MAGIC   ,a.pass_defense_1_player_id
# MAGIC   ,a.pass_defense_1_player_name
# MAGIC   ,a.pass_defense_2_player_id
# MAGIC   ,a.pass_defense_2_player_name
# MAGIC   ,a.fumbled_1_team
# MAGIC   ,a.fumbled_1_player_id
# MAGIC   ,a.fumbled_1_player_name
# MAGIC   ,a.fumbled_2_player_id
# MAGIC   ,a.fumbled_2_player_name
# MAGIC   ,a.fumbled_2_team
# MAGIC   ,a.fumble_recovery_1_team
# MAGIC   ,a.fumble_recovery_1_yards
# MAGIC   ,a.fumble_recovery_1_player_id
# MAGIC   ,a.fumble_recovery_1_player_name
# MAGIC   ,a.fumble_recovery_2_team
# MAGIC   ,a.fumble_recovery_2_yards
# MAGIC   ,a.fumble_recovery_2_player_id
# MAGIC   ,a.fumble_recovery_2_player_name
# MAGIC   ,a.return_team
# MAGIC   ,a.return_yards
# MAGIC   ,a.penalty_team
# MAGIC   ,a.penalty_player_id
# MAGIC   ,a.penalty_player_name
# MAGIC   ,a.penalty_yards
# MAGIC   ,a.replay_or_challenge
# MAGIC   ,a.replay_or_challenge_result
# MAGIC   ,a.penalty_type
# MAGIC   ,a.defensive_two_point_attempt
# MAGIC   ,a.defensive_two_point_conv
# MAGIC   ,a.defensive_extra_point_attempt
# MAGIC   ,a.defensive_extra_point_conv
# MAGIC   ,a.season
# MAGIC FROM nflPBP AS A
# MAGIC INNER JOIN teamnamelookup AS B1
# MAGIC ON A.home_team = B1.orgteamabr
# MAGIC INNER JOIN teamnamelookup AS B2
# MAGIC ON A.away_team = B2.orgteamabr
# MAGIC INNER JOIN teamnamelookup AS B3
# MAGIC ON A.posteam = B3.orgteamabr
# MAGIC INNER JOIN teamnamelookup AS B4
# MAGIC ON A.defteam = B4.orgteamabr
# MAGIC INNER JOIN teamnamelookup AS B5
# MAGIC ON A.side_of_field = B5.orgteamabr;