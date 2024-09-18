# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def ingestion_time(df):
    df = df.withColumn("ingestion_time", current_timestamp())
    return df

