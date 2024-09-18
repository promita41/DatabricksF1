-- Databricks notebook source
create database if not exists demo

-- COMMAND ----------

show databases

-- COMMAND ----------

desc database extended demo

-- COMMAND ----------

show current database

-- COMMAND ----------

use demo

-- COMMAND ----------

show current database

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC demo_df=spark.read.parquet(f"{presentation}/output/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC demo_df.write.mode("overwrite").format("parquet").saveAsTable("demo.races_reselt_python")

-- COMMAND ----------

create table if not exists demo.races_result_sql
as
select * from demo.races_reselt_python
where race_year=2020

-- COMMAND ----------

select * from demo.races_result_sql

-- COMMAND ----------

desc extended demo.races_result_sql

-- COMMAND ----------

-- MAGIC %python
-- MAGIC demo_df.write.mode("overwrite")\
-- MAGIC     .option("path",(f"{presentation}/races_result_ext_python/"))\
-- MAGIC         .format("parquet")\
-- MAGIC         .saveAsTable("demo.races_result_ext_python")

-- COMMAND ----------

desc extended demo.races_result_ext_python

-- COMMAND ----------

select * from demo.races_result_ext_python

-- COMMAND ----------

create table if not exists demo.races_result_ext_sql
(race_year int,
race_name string,
race_date date,
circuit_location string,
driver_name	string,
driver_number	string,
driver_nationality	string,
team	string,
grid	int,
fastest_lap	string,
race_time	string,
points	float,
position	int,
ingestion_time	timestamp
)
using parquet
location '/mnt/formula1122sa/presentation/races_result_ext_sql'

-- COMMAND ----------

insert into demo.races_result_ext_sql

select * from demo.races_result_ext_python
where race_year=2020

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

drop table demo.races_result_ext_sql

-- COMMAND ----------

select * from demo.races_result_ext_sql
