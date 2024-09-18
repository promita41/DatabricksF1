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

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

# COMMAND ----------

pit_stops_schema=StructType([StructField("raceId",IntegerType(),False),
                           StructField("driverId",IntegerType(),True),
                           StructField("stop",StringType(),True),
                           StructField("lap",IntegerType(),True),
                           StructField("time",StringType(),True),
                           StructField("duration",StringType(),True),
                           StructField("milliseconds",IntegerType(),True)
                           
                          
                           ])

# COMMAND ----------

pit_stops_df=spark.read.option("multiLine",True).schema(pit_stops_schema).json(f"{raw}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## rename and add cols

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

pit_stops_rename_df=ingestion_time(pit_stops_df.withColumnRenamed("driverId","driver_id")\
                    .withColumnRenamed("raceId","race_id"))\
                         .withColumn("source",lit(v_source))
                       

# COMMAND ----------

# MAGIC %md
# MAGIC ## write data

# COMMAND ----------

pit_stops_rename_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

dbutils.notebook.exit("success")
