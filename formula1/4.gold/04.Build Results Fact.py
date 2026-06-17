# Databricks notebook source
# MAGIC %md
# MAGIC ## Build Results Fact table
# MAGIC 1. Read results data from silver schema
# MAGIC 2. Read sprints data from silver schema
# MAGIC 3. Add new column session_type with values RACE or SPRINT
# MAGIC 4. UNION results data with sprints data
# MAGIC 5. Derive new columns as 
# MAGIC     - is_win -> Indicates that whether driver won the race or not
# MAGIC     - is_podium -> Indicates that driver finishes in top 3
# MAGIC     - has_points -> Indicates that the driver has scored points
# MAGIC 6. Write final data to gold as fact_session_results table

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Read results data from silver schema
# MAGIC ### 2. Read sprints data from silver schema
# MAGIC ### 3. Add new column session_type with values RACE or SPRINT

# COMMAND ----------

# reading results data 
# adding new columns with "RACE" value
# drop unwanted columns

from pyspark.sql.functions import lit

results_df = spark.table("formula1.silver.results")\
                .withColumn("session_type", lit("RACE"))\
                .drop("race_name", "race_date", "ingestion_timestamp", "source_file")

# COMMAND ----------

# reading sprints data
# adding new column as session_type with "SPRINT" value
# drop unwanted columns

sprints_df = spark.table("formula1.silver.sprints")\
                    .withColumn("session_type", lit("SPRINT"))\
                    .drop("race_name", "race_date", "ingestion_timestamp", "source_file")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. UNION results data with sprints data
# MAGIC ### 5. Derive new columns

# COMMAND ----------

# joining results and sprints dataframes with unionByName
# adding new columns as is_win, is_podium, has_points

from pyspark.sql.functions import col

fact_results_session_df = results_df.unionByName(sprints_df)\
                                    .withColumn("is_win", col("finish_position") == 1)\
                                    .withColumn("is_podium", col("finish_position").between(1,3))\
                                    .withColumn("has_points", col("points") > 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Write final data to gold as fact_session_results table

# COMMAND ----------

fact_results_session_df.\
    write.\
    format("delta").\
    mode("overwrite").\
    saveAsTable("formula1.gold.fact_results_session")

# COMMAND ----------

display(spark.table("formula1.gold.fact_results_session"))