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

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,DateType

# COMMAND ----------

races_schema = StructType([StructField("raceId",IntegerType(),False),
                              StructField("year",IntegerType(),True),
                              StructField("round",IntegerType(),True),
                              StructField("circuitId",IntegerType(),True),
                              StructField("name",StringType(),True),
                              StructField("date",DateType(),True),
                              StructField("time",StringType(),True),
                              StructField("url",StringType(),True)
                              ])

# COMMAND ----------

races_df=spark.read.schema(races_schema).csv(f"{raw}/races.csv",header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Select only required cols

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_selected_df=races_df.select(col("raceId"), col("year"), col("round"), col("name"),col("circuitId"), col("date"), col("time"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## rename columns

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("year", "race_year")\
    .withColumnRenamed("circuitId", "circuit_id")
    

# COMMAND ----------

# MAGIC %md
# MAGIC # Add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit,concat,to_timestamp

# COMMAND ----------

races_final_df = ingestion_time(races_renamed_df.withColumn("race_timestamp",to_timestamp(concat(col("date"),lit(" "),col("time")),"yyyy-MM-dd HH:mm:ss")).drop("date","time"))\
     .withColumn("source",lit(v_source))

# COMMAND ----------

# MAGIC %md
# MAGIC # write data in parquet format

# COMMAND ----------

races_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("success")
