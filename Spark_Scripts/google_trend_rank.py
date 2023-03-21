# Databricks notebook source
# Databricks Notebook source

# COMMAND ----------

# MAGIC %run ../config/constants

# COMMAND ----------

# DBTITLE 1,Create rank calculation Funktion
# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW Google_Trends_Rank_Calculation AS
# MAGIC 
# MAGIC WITH Sum_Rank_Calculation AS (
# MAGIC   SELECT
# MAGIC     C.Country_Name,
# MAGIC     G.Keyword,
# MAGIC     G.Search_Terms,
# MAGIC     G.Time_Frame,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY G.Time_Frame,
# MAGIC       C.Country_Name
# MAGIC       ORDER BY
# MAGIC         G.Search_Terms DESC,
# MAGIC         C.Country_Name DESC
# MAGIC     ) AS Rank,
# MAGIC     SUM(Search_Terms) OVER (
# MAGIC       PARTITION BY G.Time_Frame,
# MAGIC       C.Country_Name
# MAGIC     ) AS Total_Sum
# MAGIC   FROM
# MAGIC     google.GoogleTrends G
# MAGIC     LEFT JOIN google.country C ON G.Geo_Code = C.Geo_Code
# MAGIC   WHERE
# MAGIC     C.Country_Name is not null
# MAGIC )
# MAGIC SELECT
# MAGIC   Country_Name,
# MAGIC   Keyword,
# MAGIC   Search_Terms,
# MAGIC   Time_Frame,
# MAGIC   Rank,
# MAGIC   CAST(((Search_Terms * 100) / Total_Sum) AS DECIMAL(10, 2)) AS Rank_in_percent
# MAGIC FROM
# MAGIC   Sum_Rank_Calculation
# MAGIC ORDER BY
# MAGIC   Country_Name,
# MAGIC   Time_Frame,
# MAGIC   Rank

# COMMAND ----------

# DBTITLE 1,Save results into a delta table
analytics_delta_table_name = "GoogleTrendsRank"
analytics_delta_table_path = f"{MOUNT_POINT}/{ANALYTICS_DELTA_TABLE_BASE_FOLDER}/{analytics_delta_database_name}/{analytics_delta_table_name}.delta"
df_spark_googletrends_rank = spark.sql('''
                                    SELECT * FROM Google_Trends_Rank_Calculation
                                            ''')
df_spark_googletrends_rank.write.saveAsTable(
    name=f"`{analytics_delta_database_name}`.`{analytics_delta_table_name}`",
    format="delta", # save files as delta format
    path=analytics_delta_table_path,
    overwriteSchema="true",
    mode="overwrite",
)

# COMMAND ----------

# DBTITLE 1,Vacuum delta table and drop view
# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS Google_Trends_Rank_Calculation;
# MAGIC VACUUM google.GoogleTrendsRank;
