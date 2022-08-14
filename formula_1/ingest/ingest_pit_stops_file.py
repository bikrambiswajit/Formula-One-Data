# Databricks notebook source
# MAGIC %md
# MAGIC ####Read the JSON file using spark dataframe reader API

# COMMAND ----------

# MAGIC %run "../includes/configuration" 

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), True),
                                StructField("driverId", IntegerType(), True),
                                   StructField("stop", StringType(), True),
                                   StructField("lap", IntegerType(), True),
                                   StructField("time", StringType(), True),
                                   StructField("duration", StringType(), True),
                                   StructField("milliseconds", IntegerType(), True),])

# COMMAND ----------

pit_stops_df = spark.read.schema(pit_stops_schema).json(f"{raw_folder_path}/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Renaming and adding the required columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pit_stops_ingst_df = add_ingestion_date(pit_stops_df)

# COMMAND ----------

pit_stops_final_df = pit_stops_ingst_df.withColumnRenamed("raceId", "race_id") \
                                .withColumnRenamed("driverId", "driver_id") \
                                

# COMMAND ----------

# MAGIC %md
# MAGIC ####Writing the final Dataframe in parquet format

# COMMAND ----------

pit_stops_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/formula1race02/processed/pit_stops

# COMMAND ----------

dbutils.notebook.exit("Success")
