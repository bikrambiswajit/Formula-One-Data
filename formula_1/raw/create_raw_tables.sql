-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS raw;

-- COMMAND ----------

DROP TABLE IF EXISTS raw.circuits;
CREATE TABLE IF NOT EXISTS raw.circuits(circuitid INT,
circuitref STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING)
USING csv
OPTIONS(path "/mnt/formula1race02/raw/circuits.csv", header true);

-- COMMAND ----------

SELECT * FROM raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Races File

-- COMMAND ----------

DROP TABLE IF EXISTS raw.races;
CREATE TABLE IF NOT EXISTS raw.races(raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING)
USING csv
OPTIONS(path "/mnt/formula1race02/raw/races.csv", header true);

-- COMMAND ----------

SELECT * from raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Constructors File

-- COMMAND ----------

DROP TABLE IF EXISTS raw.constructors;
CREATE TABLE IF NOT EXISTS raw.constructors(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING)
USING json
OPTIONS(path "/mnt/formula1race02/raw/constructors.json")

-- COMMAND ----------

SELECT * FROM raw.constructors

-- COMMAND ----------

DROP TABLE IF EXISTS raw.drivers;
CREATE TABLE IF NOT EXISTS raw.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING
)
USING json
OPTIONS(path "/mnt/formula1race02/raw/drivers.json")

-- COMMAND ----------

SELECT * FROM raw.drivers;

-- COMMAND ----------

DROP TABLE IF EXISTS raw.results;
CREATE TABLE IF NOT EXISTS raw.results(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
grid INT,
position INT,
positionText STRING,
positionOrder STRING,
points FLOAT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING)
USING json
OPTIONS(path "/mnt/formula1race02/raw/results.json")

-- COMMAND ----------

select * from raw.results

-- COMMAND ----------

DROP TABLE IF EXISTS raw.pit_stops;
CREATE TABLE IF NOT EXISTS raw.pit_stops(
raceId INT,
driverId INT,
stop STRING,
lap INT,
time STRING,
duration STRING,
milliseconds INT
)
USING json
OPTIONS(path "/mnt/formula1race02/raw/pit_stops.json", multiline true);

-- COMMAND ----------

select * from raw.pit_stops

-- COMMAND ----------

DROP TABLE IF EXISTS raw.lap_times;
CREATE TABLE IF NOT EXISTS raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT)
using csv
OPTIONS(path "/mnt/formula1race02/raw/lap_times", header true)

-- COMMAND ----------

SELECT * FROM raw.lap_times

-- COMMAND ----------

DROP TABLE IF EXISTS raw.qualifying;
CREATE TABLE IF NOT EXISTS raw.qualifying(
qualifyId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING
)
using json
OPTIONS(path "/mnt/formula1race02/raw/qualifying", multiline true)

-- COMMAND ----------

SELECT * FROM raw.qualifying

-- COMMAND ----------


