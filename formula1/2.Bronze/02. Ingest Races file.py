# Databricks notebook source
# MAGIC %md
# MAGIC ## 1. Ingest races.csv file
# MAGIC 1. Read the file using dataframe reader API
# MAGIC 2. Add Metadata columns
# MAGIC     - Source file
# MAGIC     - Ingestion timestamp
# MAGIC 3. Write to bronze schema as delta table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-1: Read csv file using dataframe reader API
# MAGIC

# COMMAND ----------

# DBTITLE 1,1.1  Defining the schema explicitly
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

## defing schme explicitly
races_schema = StructType([
                                StructField('season', IntegerType()),
                                StructField('round', IntegerType()),
                                StructField('url', StringType()),
                                StructField('raceName', StringType()),
                                StructField('date', StringType()),
                                StructField('circuitId', StringType())
                         ])


# COMMAND ----------

# DBTITLE 1,1.2 Reading the file with defined schema
races_df = spark.read.\
                format('csv').\
                option('header', 'True').\
                schema(races_schema).\
                load('/Volumes/formula1/landing/files/races.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-2:Add metadata columns

# COMMAND ----------

from pyspark.sql import functions as F

races_final_df = races_df.withColumn('ingestion_timestamp', F.current_timestamp()).\
                                withColumn('source_file', F.col('_metadata.file_path'))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-3: Write to Bronze schema as delta table

# COMMAND ----------

races_final_df.\
    write.\
    format('delta').\
    mode('overwrite').\
    saveAsTable("formula1.bronze.races")

# COMMAND ----------

display(spark.table("formula1.bronze.races"))