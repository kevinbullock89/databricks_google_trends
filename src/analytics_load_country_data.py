# Databricks notebook source
# MAGIC %run ../config/constants

# COMMAND ----------

import pandas as pd
import pycountry

# COMMAND ----------

def country():
    geo_country_dict = {}
    for country in pycountry.countries:
        geo_country_dict[country.alpha_2] = country.name

    # Create pandas DataFrame from dictionary
    countrys_df = pd.DataFrame(list(geo_country_dict.items()), columns=['Geo_Code', 'Country_Name'])
    
    return countrys_df

# COMMAND ----------

analytics_delta_table_name = "Country"
analytics_delta_table_path = f"{MOUNT_POINT}/{ANALYTICS_DELTA_TABLE_BASE_FOLDER}/{analytics_delta_database_name}/{analytics_delta_table_name}.delta"
df_spark_country = spark.createDataFrame(country()) # Convert Pandas Datafram to Spark Dataframe
df_spark_country.write.saveAsTable(
    name=f"`{analytics_delta_database_name}`.`{analytics_delta_table_name}`",
    format="delta", # save files as delta format
    path=analytics_delta_table_path,
    overwriteSchema="true",
    mode="overwrite",
)

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM google.Country;
