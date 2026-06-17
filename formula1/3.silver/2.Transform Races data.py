# Databricks notebook source
# MAGIC %md
# MAGIC ## Transform Races Data
# MAGIC 1. Read bronze Races table
# MAGIC 2. Keep only the required columns for analytics (Drop URL column)
# MAGIC 3. Standadize column names using snake_case (racename -> race_name, circuitid -> circuit_id)
# MAGIC 4. Rename columns to make them more meaningfull (date -> race_date)
# MAGIC 5. Remove duplicate records
# MAGIC 6. Transform values of columns race_name to Title_Case
# MAGIC 7. Write the transformed data into silver schema races table

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.Read bronze Races table

# COMMAND ----------

# readig the races table from bronze schema
 
races_df = spark.table("formula1.bronze.races")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.Keep only the required columns for analytics (Drop URL column)

# COMMAND ----------

# selecting all columns except URL column

from pyspark.sql.functions import col

races_selected_df = races_df.select(col("season"),
                                    col("round"),
                                    col("raceName"),
                                    col("date"),
                                    col("circuitId"),
                                    col("ingestion_timestamp"),
                                    col("source_file")
                                )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3&4. Standadize column names using snake_case (racename -> race_name, circuitid -> circuit_id)

# COMMAND ----------

# renaming the columns using withColumnsRenamed method

races_renamed_df = races_selected_df.withColumnsRenamed({"raceName": "race_name",
                                                         "circuitId": "circuit_id",
                                                         "date": "race_date"
                                                        })

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.Remove duplicate records

# COMMAND ----------

# removing duplicate records using dropDuplicates method

races_distinct_df = races_renamed_df.dropDuplicates(["season", "round"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.Transform values of columns race_name to Title_Case

# COMMAND ----------

# transforming the values in the race_name column to Title_Case with initcap method

from pyspark.sql.functions import initcap

races_final_df = races_distinct_df.withColumn("race_name", initcap(col("race_name")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.Write the transformed data into silver schema races table

# COMMAND ----------

# writing the final data to races table in silver schema 

races_final_df.\
    write.\
    format("delta").\
    mode("overwrite").\
    saveAsTable("formula1.silver.races")

# COMMAND ----------

# reading the final data from races table from silver schema

display(spark.table("formula1.silver.races"))