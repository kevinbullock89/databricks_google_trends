# Databricks notebook source
# Databricks Notebook source

# COMMAND ----------

# constants
MOUNT_POINT = "dbfs:/mnt/dls"
 
ANALYTICS_DELTA_DATABASE_NAME_PREFIX = "analytics"
ANALYTICS_DELTA_TABLE_BASE_FOLDER = "analytics"
ANALYTICS_DELTA_DATABASE_BASE_FOLDER = "analytics"

analytics_delta_database_name = f"google"
analytics_delta_database_path = f"{MOUNT_POINT}/{ANALYTICS_DELTA_DATABASE_BASE_FOLDER}/{analytics_delta_database_name}"
