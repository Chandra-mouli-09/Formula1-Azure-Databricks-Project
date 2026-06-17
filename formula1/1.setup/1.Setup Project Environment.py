# Databricks notebook source
# MAGIC %md
# MAGIC ### Set-up the project environment for the Formula1 project
# MAGIC - 1.Create Catalog Formula1
# MAGIC - 2.Create Schemas Landing, Bronze, Silver and Gold
# MAGIC - 3.Create Volume files in the Landing Schema

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create catalog Formula1

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS Formula1;
# MAGIC    

# COMMAND ----------

# MAGIC %sql
# MAGIC -- checking catalogs
# MAGIC show catalogs

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Schemas Landing, Bronze, Silver and Gold

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS  Formula1.landing;
# MAGIC CREATE SCHEMA IF NOT EXISTS  Formula1.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS  Formula1.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS  Formula1.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- checking schemas
# MAGIC use catalog Formula1;    
# MAGIC
# MAGIC show schemas

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Volume Files

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME formula1.landing.files;

# COMMAND ----------

# MAGIC %fs ls /Volumes/formula1/landing/files/