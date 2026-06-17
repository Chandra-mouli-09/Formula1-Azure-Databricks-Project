# Databricks notebook source
# MAGIC %md 
# MAGIC ## Bild Races Dimension
# MAGIC 1. Read Races data from silver
# MAGIC 2. Read Circuits data from silver
# MAGIC 3. Join the data from races with circuits using circuit_id
# MAGIC 4. Select required columns
# MAGIC     - races.season
# MAGIC     - races.round
# MAGIC     - races.race_name
# MAGIC     - races.race_date
# MAGIC     - circuits.circuit_name
# MAGIC     - circuits.locality
# MAGIC     - circuits.country
# MAGIC 5. Write the transformed data to gold dim_races table

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1&2. Read Races and Circuits data from silver

# COMMAND ----------

# reading source tables from silver schema

races_df = spark.table("formula1.silver.races")
circuits_df = spark.table("formula1.silver.circuits")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Join the data from races with circuits using circuit_id
# MAGIC ### 4. Select required columns

# COMMAND ----------

# creating dimension table by joining races and circuits tables
# selecting required columns using select method

dim_races_df = races_df.join(
                             circuits_df, 
                             races_df.circuit_id == circuits_df.circuit_id, 
                             "inner").\
                        select(
                                races_df.season,
                                races_df.round,
                                races_df.race_name,
                                races_df.race_date,
                                circuits_df.circuit_name,
                                circuits_df.locality,
                                circuits_df.country
                                )


# COMMAND ----------

# MAGIC %md
# MAGIC ###  5. Write the transformed data to gold dim_races table

# COMMAND ----------

# write final data as dimensions table into gold schema

dim_races_df.\
    write.\
    format("delta").\
    mode("overwrite").\
    saveAsTable("formula1.gold.dim_races")


# COMMAND ----------

display(spark.table("formula1.gold.dim_races"))