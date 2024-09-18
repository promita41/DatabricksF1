-- Databricks notebook source
use f1_presentation

-- COMMAND ----------

create or replace temp view drivers_standinf_1989
as
select * from drivers_standings
where race_year = 1989

-- COMMAND ----------

show tables 

-- COMMAND ----------

desc drivers_standings

-- COMMAND ----------

create or replace temp view drivers_standinf_1990
as
select * from drivers_standings
where race_year = 1990

-- COMMAND ----------

select * from drivers_standinf_1990
inner join drivers_standinf_1989
on drivers_standinf_1990.driver_name = drivers_standinf_1989.driver_name

-- COMMAND ----------

select * from drivers_standinf_1990
right join drivers_standinf_1989
on drivers_standinf_1990.driver_name = drivers_standinf_1989.driver_name

-- COMMAND ----------

select * from drivers_standinf_1990
anti join drivers_standinf_1989
on drivers_standinf_1990.driver_name = drivers_standinf_1989.driver_name

-- COMMAND ----------

select * from drivers_standinf_1990
semi join drivers_standinf_1989
on drivers_standinf_1990.driver_name = drivers_standinf_1989.driver_name
