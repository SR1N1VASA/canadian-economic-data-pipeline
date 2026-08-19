# Databricks notebook source
from pyspark.sql import functions as F

catalog_name = spark.catalog.currentCatalog()

silver_table = (
    f"{catalog_name}.default.boc_silver_exchange_rates"
)

silver_df = spark.table(silver_table)

null_date_count = (
    silver_df
    .filter(F.col("observation_date").isNull())
    .count()
)

null_value_count = (
    silver_df
    .filter(F.col("value").isNull())
    .count()
)

duplicate_count = (
    silver_df
    .groupBy("observation_date", "series")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

invalid_value_count = (
    silver_df
    .filter(F.col("value") <= 0)
    .count()
)

checks = {
    "null_dates": null_date_count,
    "null_values": null_value_count,
    "duplicates": duplicate_count,
    "invalid_values": invalid_value_count,
}

for check_name, count in checks.items():
    print(f"{check_name}: {count}")

failed_checks = {
    name: count
    for name, count in checks.items()
    if count > 0
}

assert not failed_checks, (
    f"Data quality checks failed: {failed_checks}"
)

print("All data quality checks passed.")