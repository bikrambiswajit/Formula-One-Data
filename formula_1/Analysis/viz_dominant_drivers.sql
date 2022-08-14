-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:black;text-allign:center;font-family:Ariel">Report on Dominant Drivers</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_domiant_drivers
AS
SELECT driver_name, COUNT(1) AS total_races, SUM(calculated_points) AS total_points, AVG(calculated_points) AS average_points, RANK() OVER(ORDER BY AVG(calculated_points) DESC) driver_rank
FROM f1_processed.calculated_race_results
WHERE race_year BETWEEN 2011 AND 2021
GROUP BY driver_name
HAVING COUNT(1) >= 100
ORDER BY average_points DESC

-- COMMAND ----------

SELECT * FROM v_domiant_drivers

-- COMMAND ----------



-- COMMAND ----------

SELECT race_year, driver_name, COUNT(1) AS total_races, SUM(calculated_points) AS total_points, AVG(calculated_points) AS average_points
FROM f1_processed.calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM v_domiant_drivers WHERE driver_rank <=5)
GROUP BY race_year, driver_name
ORDER BY race_year, average_points DESC

-- COMMAND ----------

SELECT race_year, driver_name, COUNT(1) AS total_races, SUM(calculated_points) AS total_points, AVG(calculated_points) AS average_points
FROM f1_processed.calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM v_domiant_drivers WHERE driver_rank <=5)
GROUP BY race_year, driver_name
ORDER BY race_year, average_points DESC

-- COMMAND ----------


