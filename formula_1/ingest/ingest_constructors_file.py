# Databricks notebook source
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

# MAGIC %run "../includes/configuration" 

# COMMAND ----------

# MAGIC %run "../includes/common_functions" 

# COMMAND ----------

constructors_df = spark.read.option("header", True).schema(constructors_schema).json(f"{raw_folder_path}/constructors.json")

# COMMAND ----------

constructors_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Drop the url column

# COMMAND ----------

constructors_dropped_df = constructors_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Rename columns and ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructors_ingst_df = add_ingestion_date(constructors_dropped_df)

# COMMAND ----------

constructors_final_df = constructors_ingst_df.withColumnRenamed("constructorId", "constructor_id") \
                                               .withColumnRenamed("constructorRef", "constructor_ref")

# COMMAND ----------

    display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####write the dataframe to parquet file

# COMMAND ----------

constructors_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")
