# Databricks notebook source
# MAGIC %md
# MAGIC ## Transform Sprints data
# MAGIC 1. Read Sprints data from bronze schema
# MAGIC 2. Keep only the required columns(Drop URL column)
# MAGIC 3. Standardise column names using snake_case(constructorId -> constructor_id, driverId -> driver_id, raceName -> race_name, positionText -> finish_position_text)
# MAGIC 4. Rename columns to make them more meaninigful (date -> race_date, grid -> grid_position, laps -> completed_laps, number -> car_number, position -> finish_position)
# MAGIC 5. Filter out rows where season, round, constructor_id or driver_id is null(Business key validation)
# MAGIC 6. Remove duplicate records
# MAGIC 7. Transform values of column race_name to Title Case
# MAGIC 8. Write the transformed data to silver schema as results table

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.Read Sprints data from bronze schema

# COMMAND ----------

# reading the results data from bronze schema

sprints_df = spark.table("formula1.bronze.sprints")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.Keep only the required columns(Drop URL column)

# COMMAND ----------

# removing unwanted columns(URL column)

from pyspark.sql.functions import col

sprints_selected_df = sprints_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3&4.Standardise column names using snake_case

# COMMAND ----------

# standardizing and renaming columns with withColumnsRenamed method

sprints_renamed_df = sprints_selected_df.withColumnsRenamed({
                                                                "constructorId": "constructor_id",
                                                                "driverId": "driver_id",
                                                                "raceName": "race_name",
                                                                "positionText": "finish_position_text",
                                                                "date": "race_date",
                                                                "grid": "grid_position",
                                                                "laps": "completed_laps",
                                                                "number": "car_number",
                                                                "position": "finish_position"
                                                            })

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.Filter out rows where season, round, constructor_id or driver_id is null(Business key validation)

# COMMAND ----------

# removing the null values to apply business key validation

sprints_filtered_df = sprints_renamed_df.filter(
    col("season").isNotNull() & 
    col("round").isNotNull() &
    col("constructor_id").isNotNull() &
    col("driver_id").isNotNull()
    )
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.Remove duplicate records

# COMMAND ----------

# removing duplicate records from the dataframe

sprints_distinct_df = sprints_filtered_df.dropDuplicates(["constructor_id", "driver_id", "round", "season"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.Transform values of column race_name to Title Case

# COMMAND ----------

# transforming values of race_name to Title Case

from pyspark.sql.functions import initcap

sprints_final_df = sprints_distinct_df.withColumn("race_name", initcap(col("race_name")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.Write the transformed data to silver schema as results table

# COMMAND ----------

# writing final data as delta table into silver schema

sprints_final_df.write.\
    format("delta").\
    mode("overwrite").\
    saveAsTable("formula1.silver.sprints")

# COMMAND ----------

# checking the final data from silver schema

display(spark.table("formula1.silver.results"))