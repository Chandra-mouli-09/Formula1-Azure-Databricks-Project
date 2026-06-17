# Databricks notebook source
# MAGIC %md
# MAGIC ## Build Constructors Dimesion
# MAGIC 1. Read Constructors data from silver schema
# MAGIC 2. Read ref_nationality_region data from gold schema
# MAGIC 3. Join the data from constructors with ref_nationality_region using   nationality
# MAGIC 4. Select required columns
# MAGIC     - constructors.constructor_id
# MAGIC     - constructors.constructor_name
# MAGIC     - constructors.nationality
# MAGIC     - ref_nationality_region.region (rename to nationality_region)
# MAGIC 5. Write the transformed data to gold as dim_constructors table

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1&2. Read constructors data from silver and reference data from gold

# COMMAND ----------

constructors_df = spark.table("formula1.silver.constructors")
ref_nationality_region_df = spark.table("formula1.gold.ref_nationality_region")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Join the data from constructors with ref_nationality_region using nationality
# MAGIC ### 4. Select required columns

# COMMAND ----------

dim_constructors_df = constructors_df.join(
                                    ref_nationality_region_df,
                                    constructors_df.nationality == ref_nationality_region_df.nationality,
                                    "left")\
                                    .select(
                                        constructors_df.constructor_id,
                                        constructors_df.constructor_name,
                                        constructors_df.nationality,
                                        ref_nationality_region_df.region.alias("nationality_region")
                                    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Write the transformed data to gold as dim_constructors table

# COMMAND ----------

dim_constructors_df.write\
    .format("delta")\
    .mode("overwrite")\
    .saveAsTable("formula1.gold.dim_constructors")

# COMMAND ----------

display(spark.table("formula1.gold.dim_constructors"))