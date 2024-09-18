-- Databricks notebook source
use f1_presentation

-- COMMAND ----------

create or replace view v_dominant_drivers
as
select 

name,
sum(total_calculated_point) as total_points,
count(1) as total_races,
avg(total_calculated_point) as avg_points,
rank() over(order by avg(total_calculated_point) desc) as rank
 from calculated_race_result
 --where race_year between 2011 and 2020
 group by name
 having total_races>=50
 order by avg_points desc

-- COMMAND ----------

show tables

-- COMMAND ----------

select * from v_dominant_drivers

-- COMMAND ----------



with cte (name,total_points,total_races,avg_points,rank) 
as 
(
select 
name,
sum(total_calculated_point) as total_points,
count(1) as total_races,
avg(total_calculated_point) as avg_points,
rank() over(order by avg(total_calculated_point) desc) as rank
 from calculated_race_result
 --where race_year between 2011 and 2020
 group by name
 having total_races>=50
 order by avg_points desc) 



select
r.race_year, 
r.name,
sum(r.total_calculated_point) as total_points,
count(1) as total_races,
avg(r.total_calculated_point) as avg_points


 from calculated_race_result as r
 join cte on r.name = cte.name
 where cte.rank <=10
 group by r.race_year, 
r.name

 order by race_year,avg_points desc

-- COMMAND ----------

select * from tv_calculated_race_result

-- COMMAND ----------

select 
name,
sum(total_calculated_point) as total_points,
count(1) as total_races,
avg(total_calculated_point) as avg_points,
rank() over(order by avg(total_calculated_point) desc) as rank
 from calculated_race_result
 --where race_year between 2011 and 2020
 group by name
 having total_races>=50
 order by avg_points desc
