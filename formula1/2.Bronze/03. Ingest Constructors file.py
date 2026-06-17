# Databricks notebook source
# MAGIC %md
# MAGIC ## 3. Ingest constructors.json file
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

## defing schme explicitly
constructor_schema = StructType([
                                   StructField('constructorId', StringType()),
                                   StructField('name', StringType()),
                                   StructField('nationality', StringType()),
                                   StructField('url', StringType())
                                ])


# COMMAND ----------

# DBTITLE 1,1.2 Reading the file with defined schema
constructor_df = spark.read.\
                format('json').\
                option('header', 'True').\
                schema(constructor_schema).\
                load('/Volumes/formula1/landing/files/constructors.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-2:Add metadata columns

# COMMAND ----------

from pyspark.sql import functions as F

constructors_final_df = constructor_df.withColumn('ingestion_timestamp', F.current_timestamp()).\
                                withColumn('source_file', F.col('_metadata.file_path'))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-3: Write to Bronze schema as delta table

# COMMAND ----------

constructors_final_df.\
    write.\
    format('delta').\
    mode('overwrite').\
    saveAsTable("formula1.bronze.constructors")

# COMMAND ----------

display(spark.table("formula1.bronze.constructors"))