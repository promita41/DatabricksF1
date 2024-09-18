# Databricks notebook source
def mount_adls(storage_account, storage_container):
        # set up the credentials
        client_id=dbutils.secrets.get(scope="formula112-scope", key="Formula112-client-id")
        tenant_id=dbutils.secrets.get(scope="formula112-scope", key="Formula112-tenant-id")
        client_secret=dbutils.secrets.get(scope="formula112-scope", key="Formula112-client-secret")
        # set up the configs
        configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
        # check if alreaady mounted and unmount if mounted
        for mount in dbutils.fs.mounts():
            if(mount.mountPoint == f"/mnt/{storage_account}/{storage_container}"):
                dbutils.fs.unmount(f"/mnt/{storage_account}/{storage_container}")
        # set up the mount
        dbutils.fs.mount(
        source = f"abfss://{storage_container}@{storage_account}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account}/{storage_container}",
        extra_configs = configs)

        display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls("formula1122sa", "demo")

# COMMAND ----------

mount_adls("formula1122sa", "raw")

# COMMAND ----------

mount_adls("formula1122sa", "presentation")

# COMMAND ----------

mount_adls("formula1122sa", "processed")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1122sa/"))
