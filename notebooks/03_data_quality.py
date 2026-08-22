# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
dbutils.widgets.text(
    "schema_name",
    "default",
    "Schema"
)

dbutils.widgets.text(
    "silver_table_name",
    "boc_silver_exchange_rates",
    "Silver Table"
)

# COMMAND ----------

from pyspark.sql import functions as F


# ---------------------------------------------------------
# 1. Read parameters
# ---------------------------------------------------------

schema_name = dbutils.widgets.get("schema_name")

silver_table_name = dbutils.widgets.get(
    "silver_table_name"
)


# ---------------------------------------------------------
# 2. Build Silver table name
# ---------------------------------------------------------

catalog_name = spark.catalog.currentCatalog()

silver_table = (
    f"{catalog_name}."
    f"{schema_name}."
    f"{silver_table_name}"
)


# ---------------------------------------------------------
# 3. Check that Silver table exists
# ---------------------------------------------------------

if not spark.catalog.tableExists(silver_table):

    raise ValueError(
        f"Silver table does not exist: {silver_table}"
    )


silver_df = spark.table(silver_table)


# ---------------------------------------------------------
# 4. Validate expected Silver columns
# ---------------------------------------------------------

required_columns = {
    "observation_date",
    "series",
    "value",
    "source_file",
    "silver_loaded_at"
}

existing_columns = set(
    silver_df.columns
)

missing_columns = (
    required_columns - existing_columns
)


if missing_columns:

    raise ValueError(
        f"Silver table is missing required columns: "
        f"{missing_columns}"
    )


print("Silver schema validation passed.")


# ---------------------------------------------------------
# 5. Null observation_date check
# ---------------------------------------------------------

null_date_count = (
    silver_df
    .filter(
        F.col("observation_date").isNull()
    )
    .count()
)


# ---------------------------------------------------------
# 6. Null value check
# ---------------------------------------------------------

null_value_count = (
    silver_df
    .filter(
        F.col("value").isNull()
    )
    .count()
)


# ---------------------------------------------------------
# 7. Duplicate business-key check
#
# Business key:
# observation_date + series
# ---------------------------------------------------------

duplicate_count = (
    silver_df
    .groupBy(
        "observation_date",
        "series"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


# ---------------------------------------------------------
# 8. Invalid value check
# ---------------------------------------------------------

invalid_value_count = (
    silver_df
    .filter(
        F.col("value") <= 0
    )
    .count()
)


# ---------------------------------------------------------
# 9. Collect results
# ---------------------------------------------------------

checks = {
    "null_dates": null_date_count,
    "null_values": null_value_count,
    "duplicates": duplicate_count,
    "invalid_values": invalid_value_count
}


for check_name, count in checks.items():

    print(
        f"{check_name}: {count}"
    )


# ---------------------------------------------------------
# 10. Fail notebook if any quality check fails
# ---------------------------------------------------------

failed_checks = {
    name: count
    for name, count in checks.items()
    if count > 0
}


assert not failed_checks, (
    f"Data quality checks failed: "
    f"{failed_checks}"
)


print(
    "All data quality checks passed."
)