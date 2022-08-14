# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest circuit.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Read the csv file using Spark DataFrame reader

# COMMAND ----------

# MAGIC %run "../includes/configuration" 

# COMMAND ----------

# MAGIC %run "../includes/common_functions" 

# COMMAND ----------

circuits_df = spark.read.option("header", True).csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1race02/raw

# COMMAND ----------

circuits_df = spark.read.option("header", True).option("inferSchema", True).csv("dbfs:/mnt/formula1race02/raw/circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

circuits_df_selected = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")

# COMMAND ----------

display(circuits_df_selected)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Renaming the columns

# COMMAND ----------

circuit_renamed_df = circuits_df_selected.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Adding ingestion timestamp to the table

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

circuits_ingst_df = add_ingestion_date(circuit_renamed_df)

# COMMAND ----------

circuits_final_df = circuits_ingst_df.withColumn("env", lit("Production"))

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Write dataframe to Datalake

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1race02/processed/circuits

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


