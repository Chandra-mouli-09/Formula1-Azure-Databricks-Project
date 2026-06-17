# Databricks notebook source
# MAGIC %md
# MAGIC ## Transform Circuits Data
# MAGIC 1. Read bronze circuits table
# MAGIC 2. Keep only the required columns for analytics (Drop URL column)
# MAGIC 3. Standadize column names using snake_case (circuitid -> circuit_id, circuitname -> circuit_name)
# MAGIC 4. Rename columns to make them more meaningfull (lat -> latitude, long -> longitude)
# MAGIC 5. Filter out rows where circuit_id is null (business key validation)
# MAGIC 6. Remove duplicate records
# MAGIC 7. Transform values of columns circuit_name and locality to Title_Case
# MAGIC 8. Write the transformed data into silver schema circuits table

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 1.Read bronze circuits table

# COMMAND ----------

# Read the bronze table with spark reader API, it will be usefull when we have multiple versions of table

# circuits_df = spark.read.option('versionAsOf', 0).table("formula1.bronze.circuits")



# COMMAND ----------

circuits_df = spark.table("formula1.bronze.circuits")


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.Keep only the required columns for analytics (Drop URL column)
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

circuits_selected_df = circuits_df.select(
    col("circuitId"),
    col("circuitName"),
    col("country"),
    col("lat"),
    col("long"),
    col("locality"),
    col("ingestion_timestamp"),
    col("source_file")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3&4. Standadize column names using snake_case (circuitid -> circuit_id, circuitname -> circuit_name)
# MAGIC

# COMMAND ----------

# rename the coulumns with withColumnRenamed method

# circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id").\
#                                        withColumnRenamed("circuitname", "circuit_name").\
#                                        withColumnRenamed("lat", "latitude").\
#                                        withColumnRenamed("long", "longitude")

# COMMAND ----------

# rename the columns with withCoulumnsRenamed method

circuits_renamed_df = circuits_selected_df.withColumnsRenamed({"circuitId": "circuit_id", 
                                                            "circuitName": "circuit_name",
                                                            "lat": "latitude",
                                                            "long": "longitude"
                                                            })

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.Filter out rows where circuit_id is null (business key validation)
# MAGIC

# COMMAND ----------

# using filter method removing null values

circuits_filtered_df = circuits_renamed_df.filter(col("circuit_id").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.Remove duplicate records

# COMMAND ----------

# for removing duplicates we can use distinct method and dropDuplicates method as well

# circuits_distinct_df = circuits_filtered_df.distinct()

circuits_distinct_df = circuits_filtered_df.dropDuplicates(["circuit_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Transform values of columns circuit_name and locality to Title_Case

# COMMAND ----------

# transforming the column values as Title_Case with initcap method

from pyspark.sql.functions import initcap

circuits_final_df = circuits_distinct_df.withColumn("circuit_name", initcap(col("circuit_name"))).\
                                        withColumn("locality", initcap(col("locality")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Write the transformed data into silver schema circuits table

# COMMAND ----------

# writing the final dataframe as delta table into the silver schema

circuits_final_df.write.\
                mode("overwrite").\
                format("delta").\
                saveAsTable("formula1.silver.circuits")

# COMMAND ----------

# reading the final data from silver schmea 

display(spark.table("formula1.silver.circuits"))