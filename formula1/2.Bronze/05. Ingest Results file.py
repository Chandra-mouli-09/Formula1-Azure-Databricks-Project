# Databricks notebook source
# MAGIC %md
# MAGIC ### 5. Ingest files from Results folder  
# MAGIC 1. Read the file with dataframe reader API
# MAGIC 2. Add metadata columns
# MAGIC     - source file
# MAGIC     - Ingestion timestamp
# MAGIC 3. Write to bronze schmea as delta table
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-1: Read the json file with dataframe reader API

# COMMAND ----------

# DBTITLE 1,1.1. Defining the schema explicitly
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType, DateType, LongType

# Define the schema for the dataframe

result_schema = StructType([
                            StructField("constructorId", StringType(), True),
                            StructField("date", DateType()),
                            StructField('driverId', StringType()),
                            StructField('grid', LongType()),
                            StructField('laps', LongType()),
                            StructField('number', LongType()),
                            StructField('points', DoubleType()),
                            StructField('position', LongType()),
                            StructField('positionText', StringType()),
                            StructField('raceName', StringType()),
                            StructField('round', LongType()),
                            StructField('season', LongType()),
                            StructField('status', StringType()),
                            StructField('url', StringType())
                        ])

# COMMAND ----------

# DBTITLE 1,1.2 Reading the file with defined schema
result_df = spark.read.format('json').\
                option('mode', 'FAILFAST').\
                schema(result_schema).\
                load("/Volumes/formula1/landing/files/results")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-2: Add metadata coulumns

# COMMAND ----------

from pyspark.sql import functions as F

result_final_df = result_df.withColumn('ingestion_timestamp', F.current_timestamp()).\
                            withColumn('source_file', F.col('_metadata.file_path'))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-3: Write to bronze schema as delta table

# COMMAND ----------

# write using dataframe reader API

result_final_df.\
    write.\
        mode('overwrite').\
            format('delta').\
                saveAsTable("formula1.bronze.results")

# COMMAND ----------

display(spark.table("formula1.bronze.results"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select season, count(*)
# MAGIC from formula1.bronze.results
# MAGIC group by season 
# MAGIC order by season