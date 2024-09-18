# Databricks notebook source
# MAGIC %run "../includes/function"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p_source","")
v_source=dbutils.widgets.get("p_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## create schema and read data

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType,FloatType

# COMMAND ----------

results_schema=StructType([StructField("constructorId",IntegerType(),True),
                           StructField("driverId",IntegerType(),True),
                           StructField("fastestLap",StringType(),False),
                           StructField("fastestLapSpeed",StringType(),True),
                           StructField("fastestLapTime",StringType(),True),
                           StructField("grid",IntegerType(),True),
                           StructField("laps",IntegerType(),True),
                           StructField("milliseconds",IntegerType(),True),
                           StructField("number",IntegerType(),True),
                           StructField("points",FloatType(),True),
                           StructField("position",IntegerType(),True),
                           StructField("positionOrder",IntegerType(),True),
                           StructField("positionText",StringType(),True),
                           StructField("raceId",IntegerType(),True),
                           StructField("rank",IntegerType(),True),
                           StructField("resultId",IntegerType(),True),
                           StructField("statusId",IntegerType(),True),
                           StructField("time",StringType(),True),
                          
                           ])

# COMMAND ----------

results_df=spark.read.schema(results_schema).json(f"{raw}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## rename and add cols

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

results_rename_df=ingestion_time(results_df.withColumnRenamed("resultId","result_id")\
                    .withColumnRenamed("raceId","race_id")\
                        .withColumnRenamed("driverId","driver_id")\
                            .withColumnRenamed("constructorId","constructor_id")\
                                .withColumnRenamed("positionText","position_text")\
                                    .withColumnRenamed("positionOrder","position_order")\
                                        .withColumnRenamed("fastestLap","fastest_lap")\
                                            .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
                                                .withColumnRenamed("fastestLapTime","fastest_lap_time"))\
                                                     .withColumn("source",lit(v_source))\
                                                         .withColumn("file_date",lit(v_file_date))
                                                  

# COMMAND ----------

# MAGIC %md
# MAGIC ## drop cols

# COMMAND ----------

results_final_df = results_rename_df.drop("statusId")

# COMMAND ----------

# MAGIC %md
# MAGIC #Incremental Refresh : Method 1

# COMMAND ----------

# for x in results_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"alter table f1_processed.results drop if exists partition(race_id={x.race_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## write data in parquet format
# MAGIC

# COMMAND ----------

# results_final_df.write.mode("append").format("parquet").partitionBy("race_id").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC #Incremental Refresh : Method 2

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

results_final_df=results_final_df.select("constructor_id",
"driver_id",
"fastest_lap",
"fastest_lap_speed",
"fastest_lap_time",
"grid",
"laps",
"milliseconds",
"number",
"points",
"position",
"position_order",
"position_text",
"rank",
"result_id",
"time",
"ingestion_time",
"source",
"file_date",
"race_id")

# COMMAND ----------

if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
    results_final_df.write.mode("overwrite").insertInto("f1_processed.results")
else:
    results_final_df.write.mode("overwrite").format("parquet").partitionBy("race_id").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.results

# COMMAND ----------

#spark.sql("drop table if exists f1_processed.results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1)
# MAGIC from f1_processed.results
# MAGIC --where file_date='2021-03-28'
# MAGIC group by race_id
# MAGIC order by race_id desc

# COMMAND ----------

dbutils.notebook.exit("success")
