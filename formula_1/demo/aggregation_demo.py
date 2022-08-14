# Databricks notebook source
# MAGIC %run "../includes/configuration" 

# COMMAND ----------

# MAGIC %md
# MAGIC ####Aggregate Functions Demo

# COMMAND ----------

race_results = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

demo_df = race_results.filter("race_year == 2000")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

demo_df.select(count("race_name")).show()

# COMMAND ----------

demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

demo_df \
.groupBy("driver_name") \
.agg(sum("points"), countDistinct("race_name").alias("number_of_races")) \
.show()

# COMMAND ----------


