# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

demo_df=spark.read.parquet(f"{presentation}/output/")

# COMMAND ----------

# MAGIC %md
# MAGIC # temp local view

# COMMAND ----------

demo_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

dbutils.widgets.dropdown(
    "Year",
    "2020",
    ["2020", "2021", "2019"]
)

# COMMAND ----------

v_year=dbutils.widgets.get("Year")

# COMMAND ----------

v_year

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(points) as total_points from v_race_results
# MAGIC where race_year=2020

# COMMAND ----------


result_year_df=spark.sql(f"select sum(points) as total_points from v_race_results \
    WHERE race_year={v_year}")

# COMMAND ----------

display(result_year_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## global temp view

# COMMAND ----------

demo_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp

# COMMAND ----------

result_year_df=spark.sql(f"select sum(points) as total_points from global_temp.gv_race_results \
    WHERE race_year={v_year}")

# COMMAND ----------

v_year

# COMMAND ----------

display(result_year_df)
