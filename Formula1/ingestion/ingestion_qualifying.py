# Databricks notebook source
# MAGIC %run "../includes/function"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p_source","")
v_source=dbutils.widgets.get("p_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ## creating schema and reading in dataframe

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

qualifying_schema=StructType([StructField("qualifyId",IntegerType(),False),
                           StructField("raceId",IntegerType(),True),
                           StructField("driverId",IntegerType(),True),
                           StructField("constructorId",IntegerType(),True),
                           StructField("number",IntegerType(),True),
                           StructField("position",IntegerType(),True),
                           StructField("q1",StringType(),True),
                           StructField("q2",StringType(),True),
                           StructField("q3",StringType(),True)
                           
                          
                           ])

# COMMAND ----------

qualifying_df=spark.read.option("multiline",True).schema(qualifying_schema).json(f"{raw}/qualifying/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## rename and add cols

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

qualifying_rename_df=ingestion_time(qualifying_df.withColumnRenamed("driverId","driver_id")\
                    .withColumnRenamed("raceId","race_id")\
                       .withColumnRenamed("qualifyId","qualify_id")\
                       .withColumnRenamed("constructorId","constructor_id"))\
                           .withColumn("source",lit(v_source))
                        

# COMMAND ----------

# MAGIC %md
# MAGIC ## write in ADLS

# COMMAND ----------

qualifying_rename_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

dbutils.notebook.exit("success")
