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

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), True),
                                StructField("driverId", IntegerType(), True),
                                   StructField("lap", IntegerType(), True),
                                   StructField("position", IntegerType(), True),
                                   StructField("time", StringType(), True),
                                   StructField("milliseconds", IntegerType(), True),])

# COMMAND ----------

lap_times_df = spark.read.schema(lap_times_schema).csv(f"{raw_folder_path}/lap_times")

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

lap_times_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Renaming and adding the required columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

lap_times_ingst_df = add_ingestion_date(lap_times_df)

# COMMAND ----------

lap_times_final_df = lap_times_ingst_df.withColumnRenamed("raceId", "race_id") \
                                .withColumnRenamed("driverId", "driver_id") 
                                

# COMMAND ----------

# MAGIC %md
# MAGIC ####Writing the final Dataframe in parquet format

# COMMAND ----------

lap_times_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/formula1race02/processed/lap_times

# COMMAND ----------

dbutils.notebook.exit("Success")
