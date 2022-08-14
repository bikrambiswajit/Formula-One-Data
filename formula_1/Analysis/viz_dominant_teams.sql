-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW v_domiant_teams
AS
SELECT team_name, COUNT(1) AS total_races, SUM(calculated_points) AS total_points, AVG(calculated_points) AS average_points, RANK() OVER(ORDER BY AVG(calculated_points) DESC) team_rank
FROM f1_processed.calculated_race_results
WHERE race_year BETWEEN 2011 AND 2021
GROUP BY team_name
HAVING COUNT(1) >= 100
ORDER BY average_points DESC

-- COMMAND ----------

SELECT * FROM v_domiant_teams

-- COMMAND ----------

SELECT race_year, team_name, COUNT(1) AS total_races, SUM(calculated_points) AS total_points, AVG(calculated_points) AS average_points
FROM f1_processed.calculated_race_results
WHERE team_name IN (SELECT team_name FROM v_domiant_teams WHERE team_rank <=10)
GROUP BY race_year, team_name
ORDER BY race_year, average_points DESC

-- COMMAND ----------


