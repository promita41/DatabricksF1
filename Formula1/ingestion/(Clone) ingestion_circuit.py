# Databricks notebook source
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
             .csv("/mnt/formula1122sa/raw/circuits.csv",header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Select only required cols

# COMMAND ----------

circuit_selected_df=circuit_df.select("circuitId","circuitRef","name","location","country","lat","lng")

# COMMAND ----------

circuit_selected_df=circuit_df.select(circuit_df.circuitId,circuit_df.circuitRef,circuit_df.name,circuit_df.location,circuit_df.country,circuit_df.lat,circuit_df.lng)

# COMMAND ----------

circuit_selected_df=circuit_df.select(circuit_df["circuitId"],circuit_df["circuitRef"],circuit_df["name"],circuit_df["location"],circuit_df["country"],circuit_df["lat"],circuit_df["lng"])

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuit_selected_df=circuit_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

display(circuit_selected_df)

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

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn("ingestion_time", current_timestamp())\
    .withColumn("env",lit("prod"))

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # write data in parquet format

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("/mnt/formula1122sa/processed/circuits")

# COMMAND ----------

df= spark.read.parquet("/mnt/formula1122sa/processed/circuits")

# COMMAND ----------

display(df)
