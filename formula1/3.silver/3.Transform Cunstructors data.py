# Databricks notebook source
# MAGIC %md
# MAGIC ## Transform Constructors Data
# MAGIC 1. Read Constructors table from bronze schema
# MAGIC 2. Keep only the required columns for analytics (Drop URL column)
# MAGIC 3. Standadize column names using snake_case (constructorId -> constructor_id)
# MAGIC 4. Rename columns to make them more meaningfull (name -> constructor_name)
# MAGIC 5. Remove duplicate records
# MAGIC 6. Transform values of column nationality to Title_Case
# MAGIC 7. Write the transformed data into silver schema races table

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 1.Read Constructors table from bronze schema

# COMMAND ----------

# reading constructors data from bronze schema

constructors_df = spark.table("formula1.bronze.constructors")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.Keep only the required columns for analytics (Drop URL column)

# COMMAND ----------

# removing unwanted column using drop method

constructors_selected_df = constructors_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3&4.Standadize column names using snake_case (constructorId -> constructor_id)

# COMMAND ----------

# renaming columns using withColumnsRenamed method

constructors_renamed_df = constructors_selected_df.withColumnsRenamed({"constructorId": "constructor_id",
                                                                       "name": "constructor_name"
                                                                    })

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.Remove duplicate records

# COMMAND ----------

# removing duplicates using dropDuplicates method

constructors_distinct_df = constructors_renamed_df.dropDuplicates(["constructor_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.Transform values of column nationality to Title_Case

# COMMAND ----------

# transforming values of the column using initcap method

from pyspark.sql.functions import initcap, col

constructors_final_df = constructors_distinct_df.withColumn("nationality", initcap(col("nationality")))


# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.Write the transformed data into silver schema races table

# COMMAND ----------

# writing final data as delta table into silver schema

constructors_final_df.write.\
    format("delta").\
    mode("overwrite").\
    saveAsTable("formula1.silver.constructors")

# COMMAND ----------

# reading the data from silver schema

display(spark.table("formula1.silver.constructors"))