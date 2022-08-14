-- Databricks notebook source
-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

USE demo;
SHOW TABLES;

-- COMMAND ----------

DESC EXTENDED race_results_python

-- COMMAND ----------

SELECT *
FROM race_results_python
WHERE race_year=2020;

-- COMMAND ----------

CREATE TABLE race_results_sql
AS
SELECT *
FROM race_results_python
WHERE race_year=2020;

-- COMMAND ----------

DROP TABLE race_results_sql

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------


