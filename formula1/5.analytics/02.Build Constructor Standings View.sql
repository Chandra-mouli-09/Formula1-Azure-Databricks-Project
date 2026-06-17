-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Build Constructor Standings
-- MAGIC #### Source files
-- MAGIC 1. fact_results_session table from gold
-- MAGIC 2. dim_constructors table from gold

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Output columns
-- MAGIC 1. season
-- MAGIC 2. constructor_id
-- MAGIC 3. constructor_name
-- MAGIC 4. nationality
-- MAGIC 5. race_starts
-- MAGIC 6. total_points
-- MAGIC 7. number of wins
-- MAGIC 8. number of podiums
-- MAGIC 9. standing position

-- COMMAND ----------

CREATE OR REPLACE VIEW formula1.gold.view_constructor_standing
AS
WITH constructor_standing_summary
AS (
SELECT  r.season,
        c.constructor_id,
        c.constructor_name,
        c.nationality,
        COUNT(*) AS race_starts,
        SUM(r.points) AS total_points,
        COUNT_IF(r.is_win) AS number_of_wins,
        COUNT_IF(r.is_podium) AS number_of_podiums
FROM formula1.gold.fact_results_session r  
JOIN formula1.gold.dim_constructors c  
ON r.constructor_id = c.constructor_id
GROUP BY r.season,
        c.constructor_id,
        c.constructor_name,
        c.nationality
)
SELECT  season,
        RANK() OVER(PARTITION BY season ORDER BY total_points DESC) AS standing_position,
        constructor_id,
        constructor_name,
        nationality,
        race_starts,
        total_points,
        number_of_wins,
        number_of_podiums
FROM constructor_standing_summary