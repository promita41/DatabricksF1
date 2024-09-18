# Databricks notebook source
# MAGIC %run "../includes/function"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

dbutils.widgets.text("p_source","")
v_source=dbutils.widgets.get("p_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ## create schema and read data

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType

# COMMAND ----------

circuit_schema = StructType([StructField("circuitId",IntegerType(),False),
                              StructField("circuitRef",StringType(),True),
                              StructField("name",StringType(),True),
                              StructField("location",StringType(),True),
                              StructField("country",StringType(),True),
                              StructField("lat",DoubleType(),True),
                              StructField("lng",DoubleType(),True),
                              StructField("alt",IntegerType(),True),
                              StructField("url",StringType(),True)
                              ])

# COMMAND ----------

#circuit_df=spark.read.option("inferSchema",True)\
circuit_df=spark.read.schema(circuit_schema)\
             .csv(f"{raw}/{v_file_date}/circuits.csv",header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Select only required cols

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuit_selected_df=circuit_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## rename columns

# COMMAND ----------

circuits_renamed_df = circuit_selected_df.withColumnRenamed("circuitRef", "circuit_ref")\
    .withColumnRenamed("circuitId", "circuit_id")\
    .withColumnRenamed("lng", "longitude")\
    .withColumnRenamed("lat", "latitude")\
    .withColumnRenamed("alt", "altitude")

# COMMAND ----------

# MAGIC %md
# MAGIC # Add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

circuits_final_df = ingestion_time(circuits_renamed_df)\
    .withColumn("Source",lit(v_source))\
        .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC # Write data in parquet format in data lake

# COMMAND ----------

# create a managed table also saves data in datalake
circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended f1_processed.circuits

# COMMAND ----------

display(spark.read.parquet(f"{processed}/circuits"))

# COMMAND ----------

dbutils.notebook.exit("success")
