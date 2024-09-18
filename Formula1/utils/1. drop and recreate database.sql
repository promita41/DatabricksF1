-- Databricks notebook source
drop database if exists f1_processed cascade;
create database if not exists f1_processed
location "/mnt/formula1122sa/processed"

-- COMMAND ----------



-- COMMAND ----------

drop database if exists f1_presentation cascade;
create database if not exists f1_presentation
location "/mnt/formula1122sa/presentation"
