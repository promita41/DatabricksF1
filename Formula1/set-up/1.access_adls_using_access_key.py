# Databricks notebook source
account_key = dbutils.secrets.get(scope="formula112-scope", key="Formula112-account-key")

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1122sa.dfs.core.windows.net",
                account_key)

# COMMAND ----------

for items in dbutils.fs.ls("abfss://demo@formula1122sa.dfs.core.windows.net/"):
    print(items.name)



# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1122sa.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1122sa.dfs.core.windows.net/circuits.csv",header=True  ))
