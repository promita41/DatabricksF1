# Databricks notebook source
# MAGIC %run "../includes/function"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p_source","")
v_source=dbutils.widgets.get("p_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ## create schema and read data
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

lap_times_schema=StructType([StructField("raceId",IntegerType(),False),
                           StructField("driverId",IntegerType(),True),
                           StructField("lap",IntegerType(),True),
                           StructField("position",IntegerType(),True),
                           StructField("time",StringType(),True),
                           StructField("milliseconds",IntegerType(),True)
                           
                          
                           ])

# COMMAND ----------

lap_times_df=spark.read.schema(lap_times_schema).csv(f"{raw}/lap_times/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## rename and add cols

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

lap_times_rename_df=ingestion_time(lap_times_df.withColumnRenamed("driverId","driver_id")\
                    .withColumnRenamed("raceId","race_id"))\
                         .withColumn("source",lit(v_source))
                        

# COMMAND ----------

# MAGIC %md
# MAGIC ## write data

# COMMAND ----------

lap_times_rename_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

dbutils.notebook.exit("success")
