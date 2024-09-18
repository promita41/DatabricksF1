-- Databricks notebook source
drop database if exists f1_presentation cascade;
create database if not exists f1_presentation
location "/mnt/formula1122sa/presentation"

-- COMMAND ----------

desc database f1_presentation
