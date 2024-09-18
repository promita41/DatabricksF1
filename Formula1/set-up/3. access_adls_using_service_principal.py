# Databricks notebook source
client_id=dbutils.secrets.get(scope="formula112-scope", key="Formula112-client-id")
tenant_id=dbutils.secrets.get(scope="formula112-scope", key="Formula112-tenant-id")
client_secret=dbutils.secrets.get(scope="formula112-scope", key="Formula112-client-secret")

# COMMAND ----------



spark.conf.set("fs.azure.account.auth.type.formula1122sa.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1122sa.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1122sa.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1122sa.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1122sa.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")


# COMMAND ----------

for items in dbutils.fs.ls("abfss://demo@formula1122sa.dfs.core.windows.net/"):
    print(items.name)



# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1122sa.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1122sa.dfs.core.windows.net/circuits.csv",header=True  ))
