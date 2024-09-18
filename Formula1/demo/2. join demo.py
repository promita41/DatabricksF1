# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed}/races").filter("race_year == 2019")\
    .withColumnRenamed("name","race_name")

# COMMAND ----------

display(races_df)

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed}/circuits")\
    .filter("circuit_id < 70")\
    .withColumnRenamed("name","circuit_name")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inner Join

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"inner")\
    .select(circuits_df.circuit_name,races_df.race_name,races_df.round)


# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # left outer join

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"left")\
    .select(circuits_df.circuit_name,races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Right outer join

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"right")\
    .select(circuits_df.circuit_name,races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Full outer join

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"full")\
    .select(circuits_df.circuit_name,races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuit_df)
