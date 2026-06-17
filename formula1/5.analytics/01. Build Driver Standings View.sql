-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Build Driver Standings
-- MAGIC #### Sources
-- MAGIC 1. fact_results_session table from gold
-- MAGIC 2. dim_drivers table from gold
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Output columns
-- MAGIC 1. season
-- MAGIC 2. driver_id
-- MAGIC 3. driver_name
-- MAGIC 4. nationality
-- MAGIC 5. race_starts
-- MAGIC 6. total_points
-- MAGIC 7. number of wins
-- MAGIC 8. number of podiums
-- MAGIC 9. standing position

-- COMMAND ----------

-- creating and storing the view in gold schema for analytics and visualization

CREATE OR REPLACE VIEW formula1.gold.view_driver_standing
AS
WITH driver_session_summary 
AS (
SELECT  r.season,
        d.driver_id,
        d.driver_name,
        d.nationality,
        COUNT(*) AS race_starts,
        SUM(r.points) AS total_points,
        COUNT_IF(r.is_win) AS number_of_wins,
        COUNT_IF(r.is_podium) AS number_of_podiums
FROM formula1.gold.fact_results_session r
JOIN formula1.gold.dim_drivers d
ON r.driver_id = d.driver_id
GROUP BY r.season,
        d.driver_id,
        d.driver_name,
        d.nationality
)
SELECT 
       season,
       RANK() OVER(PARTITION BY season ORDER BY total_points DESC) as standing_position,
       driver_id,
       driver_name,
       nationality,
       race_starts,
       total_points,
       number_of_wins,
       number_of_podiums
FROM driver_session_summary;