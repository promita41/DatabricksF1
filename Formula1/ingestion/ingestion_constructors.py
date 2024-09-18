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
# MAGIC

# COMMAND ----------

constructor_schema = ("constructorId INT, constructorRef STRING,name STRING, nationality STRING, url STRING")

# COMMAND ----------

constructor_df = spark.read.schema(constructor_schema).json(f"{raw}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## drop unwanted cols

# COMMAND ----------

construct_drop_df =constructor_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ## rename and add cols

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

constructor_final_df =ingestion_time(construct_drop_df.withColumnRenamed("constructorId","constructor_id")\
                      .withColumnRenamed("constructorRef","constructor_ref"))\
                          .withColumn("source",lit(v_source))\
                              .withColumn("file_date",lit(v_file_date))
                       

# COMMAND ----------

# MAGIC %md
# MAGIC ## # Write data in parquet format in data lake

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

display(spark.sql("select * from f1_processed.constructors"))

# COMMAND ----------



# COMMAND ----------

dbutils.notebook.exit("success")
