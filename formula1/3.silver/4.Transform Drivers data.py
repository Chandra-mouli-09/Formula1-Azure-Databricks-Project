# Databricks notebook source
# MAGIC %md
# MAGIC ## Transform Drivers data
# MAGIC 1. Read drivers data from bronze schema
# MAGIC 2. Keep only the required columns(Drop URL column)
# MAGIC 3. Standardise column names using snake_case(driverId -> driver_id, dateOfBirth -> date_of_birth)
# MAGIC 4. Concatenate name.giveName and name.familyName to create new column called driver_name and transform the value to Title Case
# MAGIC 5. Remove duplicate records
# MAGIC 6. Transform values of column nationality to Title Case
# MAGIC 7. Write the transformed data to silver schema as drivers table

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.Read drivers data from bronze schema

# COMMAND ----------

# reading drivers data using spark.table method

drivers_df = spark.table("formula1.bronze.drivers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.Keep only the required columns(Drop URL column)

# COMMAND ----------

# dropping unwanted columns using drop method
from pyspark.sql.functions import col

drivers_selected_df = drivers_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.Standardise column names using snake_case(driverId -> driver_id, dateOfBirth -> date_of_birth)

# COMMAND ----------

drivers_renamed_df = drivers_selected_df.withColumnsRenamed({"driverId": "driver_id",
                                                             "dateOfBirth": "date_of_birth"
                                                            })

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.Concatenate name.giveName and name.familyName to create new column called driver_name and transform the value to Title Case

# COMMAND ----------

# concatenating first and last name using concat_ws method and tranforming the values to Title Case with initcap method
# from resulted dataframe droping name column using drop method

from pyspark.sql.functions import concat_ws, initcap

drivers_concate_df = drivers_renamed_df.withColumn("driver_name", initcap(concat_ws(" ", col("name.givenName"), col("name.familyName"))))\
                                        .drop(col("name"))


# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.Remove duplicate records

# COMMAND ----------

# removing dupicated records using dropDuplicates method based on primary key column driver_id

drivers_distinct_df = drivers_concate_df.dropDuplicates(["driver_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.Transform values of column nationality to Title Case

# COMMAND ----------

# transforming nationality column values to Title Case using initcap method

drivers_final_df = drivers_distinct_df.withColumn("nationality", initcap(col("nationality")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.Write the transformed data to silver schema as drivers table

# COMMAND ----------

# writing final data as delta table into silver schema

drivers_final_df.write.\
    format("delta").\
    mode("overwrite").\
    saveAsTable("formula1.silver.drivers")

# COMMAND ----------

# checking the final data in drivers table from silver schema

display(spark.table("formula1.silver.drivers"))