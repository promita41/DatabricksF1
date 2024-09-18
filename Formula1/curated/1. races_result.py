# Databricks notebook source
# MAGIC %run "../includes/function"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df=spark.read.parquet(f"{processed}/circuits/").select("circuit_id","location")\
    .withColumnRenamed("location","circuit_location")
            

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

drivers_df=spark.read.parquet(f"{processed}/drivers/")\
    .select("driver_id","name","nationality","number")\
        .withColumnRenamed("name","driver_name")\
            .withColumnRenamed("nationality","driver_nationality")\
                .withColumnRenamed("number","driver_number")


# COMMAND ----------

display(drivers_df)

# COMMAND ----------

races_df=spark.read.parquet(f"{processed}/races/")\
    .select("race_id","name","race_year","race_timestamp","circuit_id")\
        .withColumnRenamed("name","race_name")\
            .withColumnRenamed("race_timestamp","race_date")
   

# COMMAND ----------

display(races_df)

# COMMAND ----------

constructors_df=spark.read.parquet(f"{processed}/constructors/")\
    .select("constructor_id","name")\
        .withColumnRenamed("name","team")

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

results_df=spark.read.parquet(f"{processed}/results/")\
    .select("race_id","grid","driver_id","constructor_id","time","fastest_lap","points","position")\
        .withColumnRenamed("time","race_time")


# COMMAND ----------

display(results_df)

# COMMAND ----------

df=ingestion_time(races_df.join(circuits_df,races_df.circuit_id==circuits_df.circuit_id,"left")\
.join(results_df,races_df.race_id==results_df.race_id,"left")\
    .join(drivers_df,results_df.driver_id==drivers_df.driver_id,"left")\
        .join(constructors_df,results_df.constructor_id==constructors_df.constructor_id,"left")\
            .select("race_year","race_name","race_date","circuit_location","driver_name","driver_number","driver_nationality","team","grid","fastest_lap","race_time","points","position"))


# COMMAND ----------

display(df)

# COMMAND ----------

df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

# COMMAND ----------

display(spark.read.parquet(f"{presentation}/race_results/"))
