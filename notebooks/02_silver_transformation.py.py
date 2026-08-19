# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql import functions as F

catalog_name = spark.catalog.currentCatalog()

bronze_table = f"{catalog_name}.default.boc_bronze_raw"
silver_table = f"{catalog_name}.default.boc_silver_exchange_rates"

bronze_df = spark.table(bronze_table)

exploded_df = bronze_df.select(
    F.explode("payload.observations").alias("observation")
)

silver_df = exploded_df.select(
    F.to_date(
        F.col("observation.d")
    ).alias("observation_date"),

    F.lit("FXUSDCAD").alias("series"),

    F.col(
        "observation.FXUSDCAD.v"
    ).cast("double").alias("value")
)

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
)

display(spark.table(silver_table))