# Databricks notebook source
# MAGIC %md
# MAGIC ### Build Drivers Dimesion
# MAGIC 1. Read Drivers data from silver schema
# MAGIC 2. Read ref_nationality_region data from gold schema
# MAGIC 3. Join the data from drivers with ref_nationality_region using   nationality
# MAGIC 4. Select required columns
# MAGIC     - drivers.driver_id
# MAGIC     - drivers.driver_name
# MAGIC     - drivers.date_of_birth
# MAGIC     - drivers.nationality
# MAGIC     - ref_nationality_region.region (rename to nationality_region)
# MAGIC 5. Write the transformed data to gold as dim_constructors table

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Read Drivers data from silver schema
# MAGIC ### 2. Read ref_nationality_region data from gold schema

# COMMAND ----------

# reading source data 

drivers_df = spark.table("formula1.silver.drivers")
ref_nationality_region_df = spark.table("formula1.gold.ref_nationality_region")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Join the data from drivers with ref_nationality_region using nationality
# MAGIC ### 4. Selecte required columns

# COMMAND ----------


# joining the dataframes with nationality
# selecting required columns

dim_drivers_df = drivers_df.join(
                                ref_nationality_region_df,
                                drivers_df.nationality == ref_nationality_region_df.nationality,
                                "left"
                                )\
                            .select(drivers_df.driver_id,
                                    drivers_df.driver_name,
                                    drivers_df.date_of_birth,
                                    drivers_df.nationality,
                                    ref_nationality_region_df.region
                                    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write the transformed data to gold as dim_constructors table

# COMMAND ----------

# write final data into gold schema as dim_drivers table

dim_drivers_df.\
    write.\
    format("delta").\
    mode("overwrite").\
    saveAsTable("formula1.gold.dim_drivers")

# COMMAND ----------

display(spark.table("formula1.gold.dim_drivers"))