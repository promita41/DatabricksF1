-- Databricks notebook source
use f1_processed

-- COMMAND ----------

select * from drivers

-- COMMAND ----------

select * , rank() over(partition by nationality order by dob desc) as rank from drivers
