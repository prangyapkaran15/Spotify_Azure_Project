# Databricks notebook source
# MAGIC %md
# MAGIC ###DimUser
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import (
    col, lower, upper, trim,
    to_timestamp, to_date, lit
)
import os 
import sys
project_path = os.path.join(os.getcwd(),'../../..')
sys.path.append(project_path)
from spotify_azure.utils.trans_functions import DataCleaner
cleaner = DataCleaner()

df_user = spark.readStream.format("cloudFiles")\
  .option("cloudFiles.format", "parquet")\
  .option("cloudFiles.schemaLocation","abfss://silver@storageapprangya.dfs.core.windows.net/DimUser/checkpoint")\
  .option("schemaValidationMode", "addNewColumns")\
  .load("abfss://bronze@storageapprangya.dfs.core.windows.net/DimUser")

#renamimg the required column 
# df_silver_table = df_user.select(
#     col("userid").alias("user_id"),
#     trim(lower(col("username"))).alias("user_name"),
#     trim(lower(col("country"))).alias("country_name"),
#     col("subscriptiontype").alias("subscription_type")    
# )

## Rename required columns ##
rename_map_user = {
    "userid" : "user_id",
    "username" : "user_name",
    "country" : "country_name",
    "subscriptiontype" : "subscription_type",
    "startdate" : "start_date",
    "enddate" : "end_date"
}
df_user = cleaner.rename_columns(df_user, rename_map_user)
df_user = cleaner.drop_columns(df_user, ["_rescued_data"])
df_user = cleaner.remove_duplicates(df_user,["user_id"])
df_user = cleaner.drop_nulls(df_user,["user_id", "user_name", "country_name"])
df_user = cleaner.trim_strings(df_user)

display(df_user)


# COMMAND ----------

df_user.writeStream.format("delta")\
       .outputMode("append")\
        .option("checkpointLocation", "abfss://silver@storageapprangya.dfs.core.windows.net/DimUser/checkpoint")\
        .trigger(once=True)\
        .option("path", "abfss://silver@storageapprangya.dfs.core.windows.net/DimUser/Data")\
        .toTable("spotify_catalog.silver.DimUser")
        

# COMMAND ----------

# MAGIC %md
# MAGIC Dim Artist
# MAGIC

# COMMAND ----------

df_artist = spark.readStream.format("cloudFiles")\
  .option("cloudFiles.format", "parquet")\
  .option("cloudFiles.schemaLocation","abfss://silver@storageapprangya.dfs.core.windows.net/DimArtist/checkpoint")\
  .option("schemaValidationMode", "addNewColumns")\
  .load("abfss://bronze@storageapprangya.dfs.core.windows.net/DimArtist")


## Rename required columns ##
rename_map_artist = {
    "artistid" : "artist_id",
    "artistname" : "artist_name",
    "country" : "country_name",
   
}
df_artist = cleaner.rename_columns(df_artist, rename_map_artist)
df_artist = cleaner.drop_columns(df_artist, ["_rescued_data"])
df_artist = cleaner.remove_duplicates(df_artist,["artist_id"])
df_artist = cleaner.drop_nulls(df_artist,["artist_id", "artist_name", "country_name"])
df_artist = cleaner.trim_strings(df_artist)

display(df_artist)


# COMMAND ----------

df_artist.writeStream.format("delta")\
       .outputMode("append")\
        .option("checkpointLocation", "abfss://silver@storageapprangya.dfs.core.windows.net/DimArtist/checkpoint")\
        .trigger(once=True)\
        .option("path", "abfss://silver@storageapprangya.dfs.core.windows.net/DimArtist/Data")\
        .toTable("spotify_catalog.silver.DimArtist")

# COMMAND ----------

from pyspark.sql.functions import when, col, lit, regexp_replace
df_track = spark.readStream.format("cloudFiles")\
  .option("cloudFiles.format", "parquet")\
  .option("cloudFiles.schemaLocation","abfss://silver@storageapprangya.dfs.core.windows.net/DimTrack/checkpoint")\
  .option("schemaValidationMode", "addNewColumns")\
  .load("abfss://bronze@storageapprangya.dfs.core.windows.net/DimTrack")


## Rename required columns ##
rename_map_track = {
    "trackid" : "track_id",
    "trackname" : "track_name",
    "artistid" : "artist_id",
    "albumname" : "album_name",
    "releasedate" : "release_date"
}
df_track = cleaner.rename_columns(df_track, rename_map_track)
df_track = cleaner.drop_columns(df_track, ["_rescued_data"])
df_track = cleaner.remove_duplicates(df_track,["track_id","artist_id"])
df_track = cleaner.drop_nulls(df_track,["track_id", "track_name", "artist_id","album_name"])
df_track = cleaner.trim_strings(df_track)
df_track = df_track.withColumn("duration_flag",when(col("duration") < lit(180),lit("short"))\
    .when((col("duration") >= lit(180)) & (col("duration") <= lit(300)),lit("medium"))\
    .otherwise(lit("long")))
df_track = df_track.withColumn("track_name",regexp_replace(col("track_name"),'-',' '))
display(df_track)


# COMMAND ----------

df_track.writeStream.format("delta")\
       .outputMode("append")\
        .option("checkpointLocation", "abfss://silver@storageapprangya.dfs.core.windows.net/DimTrack/checkpoint")\
        .trigger(once=True)\
        .option("path", "abfss://silver@storageapprangya.dfs.core.windows.net/DimTrack/Data")\
        .toTable("spotify_catalog.silver.DimTrack")

# COMMAND ----------

df_date = spark.readStream.format("cloudFiles")\
  .option("cloudFiles.format", "parquet")\
  .option("cloudFiles.schemaLocation","abfss://silver@storageapprangya.dfs.core.windows.net/DimDate/checkpoint")\
  .option("schemaValidationMode", "addNewColumns")\
  .load("abfss://bronze@storageapprangya.dfs.core.windows.net/DimDate")


## Rename required columns ##
rename_map_date = {
    "datekey" : "date_key"
}
df_date = cleaner.rename_columns(df_date, rename_map_date)
df_date = cleaner.drop_columns(df_date, ["_rescued_data"])
df_date = cleaner.remove_duplicates(df_date,["date_key"])
df_date = cleaner.drop_nulls(df_date,["date_key"])
df_date = cleaner.trim_strings(df_date)

display(df_date)


# COMMAND ----------

df_date.writeStream.format("delta")\
       .outputMode("append")\
        .option("checkpointLocation", "abfss://silver@storageapprangya.dfs.core.windows.net/DimDate/checkpoint")\
        .trigger(once=True)\
        .option("path", "abfss://silver@storageapprangya.dfs.core.windows.net/DimDate/Data")\
        .toTable("spotify_catalog.silver.DimDate")

# COMMAND ----------

df_factstream = spark.readStream.format("cloudFiles")\
  .option("cloudFiles.format", "parquet")\
  .option("cloudFiles.schemaLocation","abfss://silver@storageapprangya.dfs.core.windows.net/FactStream/checkpoint")\
  .option("schemaValidationMode", "addNewColumns")\
  .load("abfss://bronze@storageapprangya.dfs.core.windows.net/FactStream")


## Rename required columns ##
rename_map_factstream = {
    "streamid" : "stream_id",
    "userid" : "user_id",
    "trackid" : "track_id",
    "datekey" : "date_key",
    "listenduration" : "listen_duration",
    "devicetype" : "device_type",
    "streamtimestamp" : "stream_timestamp"
}
df_factstream = cleaner.rename_columns(df_factstream, rename_map_factstream)
df_factstream = cleaner.drop_columns(df_factstream, ["_rescued_data"])
df_factstream = cleaner.remove_duplicates(df_factstream,["stream_id","user_id","track_id","date_key"])
df_factstream = cleaner.trim_strings(df_factstream)

display(df_factstream)


# COMMAND ----------

df_factstream.writeStream.format("delta")\
       .outputMode("append")\
        .option("checkpointLocation", "abfss://silver@storageapprangya.dfs.core.windows.net/FactStream/checkpoint")\
        .trigger(once=True)\
        .option("path", "abfss://silver@storageapprangya.dfs.core.windows.net/FactStream/Data")\
        .toTable("spotify_catalog.silver.FactStream")
