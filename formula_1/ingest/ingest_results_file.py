# Databricks notebook source
# MAGIC %md
# MAGIC ####Read the JSON file using spark dataframe reader API

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), True),
                                StructField("raceId", IntegerType(), True),
                                   StructField("driverId", IntegerType(), True),
                                   StructField("constructorId", IntegerType(), True),
                                   StructField("number", IntegerType(), True),
                                   StructField("grid", IntegerType(), True),
                                   StructField("position", IntegerType(), True),
                                   StructField("positionText", StringType(), True),
                                   StructField("positionOrder", IntegerType(), True),
                                   StructField("points", FloatType(), True),
                                   StructField("laps", IntegerType(), True),
                                   StructField("time", StringType(), True),
                                   StructField("milliseconds", IntegerType(), True),
                                   StructField("fastestLap", IntegerType(), True),
                                   StructField("rank", IntegerType(), True),
                                   StructField("fastestLapTime", StringType(), True),
                                   StructField("fastestLapSpeed", FloatType(), True),
                                   StructField("statusId", StringType(), True),])

# COMMAND ----------

results_df = spark.read.schema(results_schema).json(f"{raw_folder_path}/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Renaming and adding the required columns

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit, current_timestamp

# COMMAND ----------

results_column_df = results_df.withColumnRenamed("resultId", "result_id") \
                                .withColumnRenamed("raceId", "race_id") \
                                .withColumnRenamed("driverId", "driver_id") \
                                .withColumnRenamed("constructorId", "constructor_id") \
                                .withColumnRenamed("positionText", "position_text") \
                                .withColumnRenamed("positionOrder", "position_order") \
                                .withColumnRenamed("fastestLap", "fastest_lap") \
                                .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                

# COMMAND ----------

results_ingsted_df = add_ingestion_date(results_column_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Removing unwanted column

# COMMAND ----------

results_final_df = results_ingsted_df.drop("statusId")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Writing the final Dataframe in parquet format

# COMMAND ----------

results_final_df.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/formula1race02/processed/results

# COMMAND ----------

dbutils.notebook.exit("Success")
