# Databricks notebook source
# MAGIC %run ../config/constants

# COMMAND ----------

# MAGIC %run /Shared/api_header

# COMMAND ----------

import pandas as pd
from pytrends.request import TrendReq as UTrendReq
import pycountry
import time

# COMMAND ----------


# use of a custom header for the pytrend request. More information here: https://stackoverflow.com/questions/50571317/pytrends-the-request-failed-google-returned-a-response-with-code-429

GET_METHOD='get'

class TrendReq(UTrendReq):
    def _get_data(self, url, method=GET_METHOD, trim_chars=0, **kwargs):
        return super()._get_data(url, method=GET_METHOD, trim_chars=trim_chars, headers=headers, **kwargs)
    
pytrend = TrendReq()

def google_trends(kw, tf):
    """google_trends(kw, tf) --> Returns a Pandas Dataframe.

    Parameters:
    arg1: the first argument. Provide list of search terms.
    arg2: the second argument. Provide list of time frames.

    With this function any number of search terms and time frames can be loaded from Google Trends. 
    More information can be found here: https://pypi.org/project/pytrends/

    """

    result = pd.DataFrame()
    # These two for loops allow it to specify any number of search terms and timeframes. 
    for kw in kw_list:
        for tf in timeframes:
            # search interest per region
            # run model for keywords 
            pytrend.build_payload([kw], 
                                cat=0,
                                timeframe = tf,
                                geo='',
                                gprop='')

            # Interest by Region
            interest_by_region  = pytrend.interest_by_region(resolution='COUNTRY', inc_low_vol=True, inc_geo_code=True)

            # Unpivot Data Frame
            melt_df = pd.melt(interest_by_region, id_vars=['geoCode'], var_name='Keyword', value_name='Search Terms')

            # add Time Frame column to the DataFrame
            melt_df['Time_Frame'] = tf 

            # Rename columns 
            melt_df = melt_df.rename(columns={'geoCode': 'Geo_Code', 'Search Terms': 'Search_Terms'})

            # Append the result in one data frame
            result = result.append(melt_df, ignore_index=True)
            
            # Wait one second after each loop to avoid: The request failed: Google returned a response with code 429
            time.sleep(1)

    return result

# COMMAND ----------

# provide your search terms
kw_list=['vpn', 'hack', 'cyber', 'security', 'wifi']

# provide your time frames
timeframes = ['now 7-d']

df_googletrends = (google_trends(kw_list,timeframes))

# COMMAND ----------

analytics_delta_table_name = "GoogleTrends"
analytics_delta_table_path = f"{MOUNT_POINT}/{ANALYTICS_DELTA_TABLE_BASE_FOLDER}/{analytics_delta_database_name}/{analytics_delta_table_name}.delta"
df_spark_googletrends = spark.createDataFrame(df_googletrends) # Convert Pandas Datafram to Spark Dataframe
df_spark_googletrends.write.saveAsTable(
    name=f"`{analytics_delta_database_name}`.`{analytics_delta_table_name}`",
    format="delta", # save files as delta format
    path=analytics_delta_table_path,
    overwriteSchema="true",
    mode="overwrite",
)

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM google.googletrends;
