# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed}/races")

# COMMAND ----------

# python way of writing
races_filter_df= races_df.filter(races_df.race_year == 2019)

# COMMAND ----------

# SQL way of writing
races_filter_df= races_df.filter("race_year == 2019 and name=='Australian Grand Prix'")

# COMMAND ----------

races_filter_df= races_df.filter((races_df.race_year == 2019) & (races_df.name=="Australian Grand Prix"))

# COMMAND ----------

display(races_filter_df)

# COMMAND ----------

races_filter_df= races_df.where((races_df.race_year == 2019) & (races_df.name=='Australian Grand Prix'))
