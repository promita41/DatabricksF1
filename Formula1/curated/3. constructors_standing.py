# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_result_df=(spark.read.parquet(f"{presentation}/race_results/"))

# COMMAND ----------

display(races_result_df)

# COMMAND ----------

from pyspark.sql.functions import sum,count,col,when
constructor_grouped_df=races_result_df.groupBy("race_year","team")\
    .agg(sum("points").alias("total_points"),
         count(when(col("position")==1,True)).alias("wins"))


# COMMAND ----------

display(constructor_grouped_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank,desc
constructor_rank_spec=Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
constructor_ranked_df=constructor_grouped_df.withColumn("rank",rank().over(constructor_rank_spec))

# COMMAND ----------

display(constructor_ranked_df)

# COMMAND ----------

constructor_ranked_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")

# COMMAND ----------

df=spark.read.parquet(f"{presentation}/constructor_standings/")

# COMMAND ----------

display(df)
