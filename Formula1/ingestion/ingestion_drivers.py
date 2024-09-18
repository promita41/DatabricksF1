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
# MAGIC ## create nested schema and read data

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

# COMMAND ----------

name_schema=StructType([StructField("forename",StringType(),True),
                        StructField("surname",StringType(),True)
                        ])

# COMMAND ----------

drivers_schema=StructType([StructField("code",StringType(),True),
                           StructField("dob",StringType(),True),
                           StructField("driverId",IntegerType(),False),
                           StructField("driverRef",StringType(),True),
                           StructField("name",name_schema,True),
                           StructField("nationality",StringType(),True),
                           StructField("number",StringType(),True),
                           StructField("url",StringType(),True)
                          
                           ])

# COMMAND ----------

drivers_df=spark.read.schema(drivers_schema).json(f"{raw}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## rename and add cols

# COMMAND ----------

from pyspark.sql.functions import col,lit,current_timestamp,concat

# COMMAND ----------

drivers_rename_df=ingestion_time(drivers_df.withColumnRenamed("driverId","driver_id")\
                    .withColumnRenamed("driverRef","driver_ref")\
                        .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname"))))\
                            .withColumn("source",lit(v_source))\
                                .withColumn("file_date",lit(v_file_date))
                            

# COMMAND ----------

# MAGIC %md
# MAGIC ## drop cols

# COMMAND ----------

drivers_final_df = drivers_rename_df.drop("name.forename","name.surname","url")

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

processed

# COMMAND ----------

display(spark.read.parquet(f"{processed}/drivers"))


# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------


