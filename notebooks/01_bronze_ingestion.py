# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql import functions as F

catalog_name = spark.catalog.currentCatalog()

raw_path = f"/Volumes/{catalog_name}/default/boc_raw/"
bronze_table = f"{catalog_name}.default.boc_bronze_raw"

raw_df = (
    spark.read
    .option("multiline", "true")
    .json(raw_path)
)

bronze_df = raw_df.withColumn(
    "bronze_loaded_at",
    F.current_timestamp()
)

(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(bronze_table)
)

display(spark.table(bronze_table))