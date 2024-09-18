-- Databricks notebook source
drop  table if exists f1_presentation.calculated_race_result

-- COMMAND ----------

create table f1_presentation.calculated_race_result
using parquet
as
select races.race_year,
constructors.name as team,
drivers.name,
results.position,
--results.points,
11-results.position as total_calculated_point

from results
join drivers on f1_processed.results.driver_id = drivers.driver_id
join constructors on f1_processed.results.constructor_id = constructors.constructor_id
join races on f1_processed.results.race_id = races.race_id
where f1_processed.results.position <=10
