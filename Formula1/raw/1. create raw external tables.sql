-- Databricks notebook source
--As no location of database is specified it willbe created under default hive metastore
create database if not exists f1_raw

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## using CSV file

-- COMMAND ----------

drop table if exists f1_raw.circuits_raw;
create table if not exists f1_raw.circuits_raw
(
circuitId integer,
circuitRef string,
name string,
location string,
country string,
lat double,
lng double,
alt integer,
url string
)
using csv
--path from where to read the data
options (path "/mnt/formula1122sa/raw/circuits.csv",header True)

-- COMMAND ----------

desc extended f1_raw.circuits_raw

-- COMMAND ----------

select * from f1_raw.circuits_raw

-- COMMAND ----------

drop table if exists f1_raw.races_raw;
create table if not exists f1_raw.races_raw
(
raceId integer,
year integer,
round integer,
circuitId integer,
name string,
date date,
time string,
url string
)
using csv
options (path "/mnt/formula1122sa/raw/races.csv",header True)

-- COMMAND ----------

select * from f1_raw.races_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## json files

-- COMMAND ----------

drop table if exists f1_raw.constructors_raw;
create table if not exists f1_raw.constructors_raw
(
constructorId integer,
constructorRef string,
name string,
nationality string,
url string
)
using json
options (path "/mnt/formula1122sa/raw/constructors.json")

-- COMMAND ----------

select * from f1_raw.constructors_raw

-- COMMAND ----------

drop table if exists f1_raw.drivers_raw;
create table if not exists f1_raw.drivers_raw
(
code string,
dob string,
driverId integer,
driverRef string,
name struct<forename:string,
surname:string>,
nationality string,
number string,
url string
)
using json
options (path "/mnt/formula1122sa/raw/drivers.json")

-- COMMAND ----------

select * from f1_raw.drivers_raw

-- COMMAND ----------

drop table if exists f1_raw.results_raw;
create table if not exists f1_raw.results_raw
(
constructorId integer,
driverId integer,
fastestLap string,
fastestLapSpeed string,
fastestLapTime string,
grid integer,
laps integer,
milliseconds integer,
number integer,
points float,
position integer,
positionOrder integer,
positionText string,
raceId integer,
rank integer,
resultId integer,
statusId integer,
time string
)
using json
options (path "/mnt/formula1122sa/raw/results.json")

-- COMMAND ----------

select * from f1_raw.results_raw

-- COMMAND ----------

drop table if exists f1_raw.pit_stops_raw;
create table if not exists f1_raw.pit_stops_raw
(
raceId integer,
driverId integer,
lap integer,
position integer,
time string,
milliseconds integer
)
using json
options (path "/mnt/formula1122sa/raw/pit_stops.json", multiLine True)

-- COMMAND ----------

select * from f1_raw.pit_stops_raw

-- COMMAND ----------

drop table if exists f1_raw.lap_times_raw;
create table if not exists f1_raw.lap_times_raw
(
raceId integer,
driverId integer,
lap integer,
position integer,
time string,
milliseconds integer
)
using csv
options (path "/mnt/formula1122sa/raw/lap_times/")

-- COMMAND ----------

select count(1) from f1_raw.lap_times_raw

-- COMMAND ----------


drop table if exists f1_raw.qualifying_raw;
create table if not exists f1_raw.qualifying_raw
(
qualifyId integer, 
raceId integer,
driverId integer,
constructorId integer,
number integer,
position integer,
q1 string,
q2 string,
q3 string
)
using json
options (path "/mnt/formula1122sa/raw/qualifying/", multiLine True)

-- COMMAND ----------

select * from f1_raw.qualifying_raw
