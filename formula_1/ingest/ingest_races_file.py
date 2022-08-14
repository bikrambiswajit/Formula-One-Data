# Databricks notebook source
# MAGIC %md
# MAGIC ##Races File

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType

# COMMAND ----------

races_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                                   StructField("year", IntegerType(), True),
                                   StructField("round", IntegerType(), True),
                                   StructField("circuitId", IntegerType(), True),
                                   StructField("name", StringType(), True),
                                   StructField("date", DateType(), True),
                                   StructField("time", StringType(), True),
                                   StructField("url", StringType(), True)
                                   ])

# COMMAND ----------

# MAGIC %md
# MAGIC #####1. Read the file

# COMMAND ----------

races_df = spark.read.option("header", True).csv(f"{raw_folder_path}/races.csv")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

races_df = spark.read.option("header", True).schema(races_schema).csv("dbfs:/mnt/formula1race02/raw/races.csv")

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

races_df.show()

# COMMAND ----------

races_selected_df = races_df.select("raceId", "year", "round", "circuitId", "name", "date", "time")

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("year", "race_year") \
.withColumnRenamed("circuitId", "circuit_id")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, lit, concat, col

# COMMAND ----------

races_timestamp_df = races_renamed_df.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

races_ingst_df = add_ingestion_date(races_timestamp_df)

# COMMAND ----------

races_ingst_df.show()

# COMMAND ----------

races_final_df = races_ingst_df.select("race_id", "race_year", "round", "circuit_id", "name", "ingestion_date", "race_timestamp")

# COMMAND ----------

races_final_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Writing the dataframe to Datalake

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy("race_year").format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1race02/processed/races

# COMMAND ----------

dbutils.notebook.exit("Success")
