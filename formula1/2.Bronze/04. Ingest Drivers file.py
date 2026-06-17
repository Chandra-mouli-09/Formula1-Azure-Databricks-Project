# Databricks notebook source
# MAGIC %md
# MAGIC ## 4. Ingest drivers.json file
# MAGIC 1. Read the file using dataframe reader API
# MAGIC 2. Add Metadata columns
# MAGIC     - Source file
# MAGIC     - Ingestion timestamp
# MAGIC 3. Write to bronze schema as delta table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-1: Read json file using dataframe reader API
# MAGIC

# COMMAND ----------

# DBTITLE 1,1.1  Defining the schema explicitly
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

## defing schme for inner fields explicitly

name_schema = StructType([
                           StructField('familyName', StringType()),
                           StructField('givenName', StringType())
                         ])
driver_schema = StructType([
                           StructField('dateOfBirth', DateType()),
                           StructField('driverId', StringType()),
                           StructField('name', name_schema),
                           StructField('nationality', StringType()),
                           StructField('url', StringType())
                        ])

# COMMAND ----------

# DBTITLE 1,1.2 Reading the file with defined schema
driver_df = spark.read.\
                format('json').\
                option('header', 'True').\
                schema(driver_schema).\
                load('/Volumes/formula1/landing/files/drivers.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-2:Add metadata columns

# COMMAND ----------

from pyspark.sql import functions as F

driver_final_df = driver_df.withColumn('ingestion_timestamp', F.current_timestamp()).\
                                withColumn('source_file', F.col('_metadata.file_path'))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-3: Write to Bronze schema as delta table

# COMMAND ----------

driver_final_df.\
    write.\
    format('delta').\
    mode('overwrite').\
    saveAsTable("formula1.bronze.drivers")

# COMMAND ----------

display(spark.table("formula1.bronze.drivers"))