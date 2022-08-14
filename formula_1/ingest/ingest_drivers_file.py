# Databricks notebook source
# MAGIC %md
# MAGIC ####Read the JSON file using spark dataframe reader API

# COMMAND ----------

# MAGIC %run "../includes/configuration" 

# COMMAND ----------

# MAGIC %run "../includes/common_functions" 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                StructField("surname", StringType(), True)])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), True),
                                   StructField("driverRef", StringType(), True),
                                   StructField("number", IntegerType(), True),
                                   StructField("code", StringType(), True),
                                   StructField("name", name_schema),
                                   StructField("dob", DateType(), True),
                                   StructField("nationality", StringType(), True),
                                   StructField("url", StringType(), True),])

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json(f"{raw_folder_path}/drivers.json")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Rename Columns and adding new columns

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit, current_timestamp

# COMMAND ----------

drivers_ingst_df = add_ingestion_date(drivers_df)

# COMMAND ----------

drivers_column_df = drivers_ingst_df.withColumnRenamed("driverId", "driver_id") \
                                .withColumnRenamed("driverRef", "driver_ref") \
                                .withColumn("name",concat(col("name.forename"), lit(" "), col("name.surname")))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Removing the unwanted columns

# COMMAND ----------

drivers_final_df = drivers_column_df.drop("url")

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/formula1race02/processed/drivers

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


