# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_source=dbutils.notebook.run("ingestion_circuit", 0,{"p_source":"Ergest API"})

# COMMAND ----------

v_source=dbutils.notebook.run("ingestion_constructors", 0,{"p_source":"Ergest API"})

# COMMAND ----------

v_source=dbutils.notebook.run("ingestion_drivers", 0,{"p_source":"Ergest API"})

# COMMAND ----------

v_source=dbutils.notebook.run("ingestion_lap_times", 0,{"p_source":"Ergest API"})

# COMMAND ----------

v_source=dbutils.notebook.run("ingestion_pit_stops", 0,{"p_source":"Ergest API"})

# COMMAND ----------

v_source=dbutils.notebook.run("ingestion_qualifying", 0,{"p_source":"Ergest API"})

# COMMAND ----------

v_source=dbutils.notebook.run("ingestion_races", 0,{"p_source":"Ergest API"})

# COMMAND ----------

v_source=dbutils.notebook.run("ingestion_results", 0,{"p_source":"Ergest API"})
