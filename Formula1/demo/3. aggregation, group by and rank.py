# Databricks notebook source
# MAGIC %run "../includes/function"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

demo_df=spark.read.parquet(f"{presentation}/output/").filter("race_year=2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import sum, count, countDistinct

# COMMAND ----------

demo_df.filter("driver_name='Lewis Hamilton'").select(sum("points"),countDistinct("race_name"))\
    .withColumnRenamed("sum(points)","total_points")\
        .withColumnRenamed("count(DISTINCT race_name)","number_of_races")\
         .show()

# COMMAND ----------

demo_df\
    .groupBy("driver_name")\
        .agg(sum("points").alias("total_points"),countDistinct("race_name").alias("number_of_races"))\
            .show()

            
              

# COMMAND ----------

# MAGIC %md
# MAGIC ## window function

# COMMAND ----------

demo_df=spark.read.parquet(f"{presentation}/output/").filter("race_year in (2020,2019)")

# COMMAND ----------

df=demo_df\
    .groupBy("race_year","driver_name")\
        .agg(sum("points").alias("total_points"),countDistinct("race_name").alias("number_of_races"))\
           

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank,desc

# COMMAND ----------

windowSpec=Window.partitionBy("race_year").orderBy(desc("total_points"))
races_ranked_df=df.withColumn("rank",rank().over(windowSpec))

# COMMAND ----------

display(races_ranked_df)
