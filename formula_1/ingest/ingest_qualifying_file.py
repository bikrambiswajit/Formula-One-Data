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

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                StructField("raceId", IntegerType(), True),
                                   StructField("driverId", IntegerType(), True),
                                   StructField("constructorId", IntegerType(), True),
                                   StructField("number", IntegerType(), True),
                                   StructField("position", IntegerType(), True),
                                   StructField("q1", StringType(), True),
                                   StructField("q2", StringType(), True),
                                  StructField("q3", StringType(), True),])

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).option("multiline", True).json(f"{raw_folder_path}/qualifying")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Renaming and adding the required columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

qualifying_ingst_df = add_ingestion_date(qualifying_df)

# COMMAND ----------

qualifying_final_df = qualifying_ingst_df.withColumnRenamed("qualifyId", "qualify_id") \
                                .withColumnRenamed("raceId", "race_id") \
                                .withColumnRenamed("driverId", "driver_id") \
                                .withColumnRenamed("constructorId", "constructor_id")
                                

# COMMAND ----------

# MAGIC %md
# MAGIC ####Writing the final Dataframe in parquet format

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/formula1race02/processed/qualifying

# COMMAND ----------

dbutils.notebook.exit("Success")
