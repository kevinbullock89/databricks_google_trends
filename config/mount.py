# Databricks notebook source
# Databricks Notebook source

# COMMAND ----------

# MAGIC %run ./constants

# COMMAND ----------

pricipal_id = dbutils.secrets.get(scope="keyVaultScope", key="databricksDataLakeAccessAppId")
principal_secret = dbutils.secrets.get(scope="keyVaultScope", key="databricksDataLakeAccessAppSecret")
tenant_id = dbutils.secrets.get(scope="keyVaultScope", key="tenantId")
storage_account_name = dbutils.secrets.get(scope="keyVaultScope", key="dataLakeStorageAccountName")
file_system_name = "dls"
MOUNT_POINT = "/mnt/dls"

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": pricipal_id,
    "fs.azure.account.oauth2.client.secret": principal_secret,
    "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/{}/oauth2/token".format(tenant_id),
}

dbutils.fs.mount(
    source="abfss://{}@{}.dfs.core.windows.net/".format(file_system_name, storage_account_name),
    mount_point=MOUNT_POINT,
    extra_configs=configs,
)
