# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_result_df=(spark.read.parquet(f"{presentation}/race_results/"))

# COMMAND ----------

display(races_result_df)

# COMMAND ----------

from pyspark.sql.functions import sum,count,col,when
drivers_grouped_df=races_result_df.groupBy("race_year","driver_name","driver_nationality","team")\
    .agg(sum("points").alias("total_points"),
         count(when(col("position")==1,True)).alias("wins"))


# COMMAND ----------

display(drivers_grouped_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank,desc
driver_rank_spec=Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
drivers_ranked_df=drivers_grouped_df.withColumn("rank",rank().over(driver_rank_spec))

# COMMAND ----------

display(drivers_ranked_df)

# COMMAND ----------

drivers_ranked_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.drivers_standings")

# COMMAND ----------



# COMMAND ----------

display(spark.read.parquet(f"{presentation}/drivers_standings/"))
