# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year =2019")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
.select(circuits_df.circuit_id, circuits_df.location, circuits_df.country, races_df.name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_left_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left") \
.select(circuits_df.circuit_id, circuits_df.location, circuits_df.country, races_df.name, races_df.round)

# COMMAND ----------

display(race_circuits_left_df)

# COMMAND ----------

race_circuits_right_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right") \
.select(circuits_df.circuit_id, circuits_df.location, circuits_df.country, races_df.name, races_df.round)

# COMMAND ----------

display(race_circuits_right_df)

# COMMAND ----------

race_circuits_full_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full") \
.select(circuits_df.circuit_id, circuits_df.location, circuits_df.country, races_df.name, races_df.round)

# COMMAND ----------

display(race_circuits_full_df)

# COMMAND ----------


