-- Databricks notebook source
SELECT team_name, COUNT(1) AS total_races, SUM(calculated_points) AS total_points, AVG(calculated_points) AS average_points
FROM f1_processed.calculated_race_results
WHERE race_year BETWEEN 2011 AND 2021
GROUP BY team_name
HAVING COUNT(1) >= 100
ORDER BY average_points DESC

-- COMMAND ----------


