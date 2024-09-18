-- Databricks notebook source
drop database if exists f1_processed cascade;
create database if not exists f1_processed
location "/mnt/formula1122sa/processed"

-- COMMAND ----------

desc database f1_processed
