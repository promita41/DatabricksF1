# Databricks notebook source
SAS_token = dbutils.secrets.get(scope="formula112-scope", key="formula112-SAS-token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1122sa.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1122sa.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1122sa.dfs.core.windows.net", SAS_token)

# COMMAND ----------

for items in dbutils.fs.ls("abfss://demo@formula1122sa.dfs.core.windows.net/"):
    print(items.name)



# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1122sa.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1122sa.dfs.core.windows.net/circuits.csv",header=True  ))
