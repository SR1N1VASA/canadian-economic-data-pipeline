# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
dbutils.widgets.text(
    "series",
    "FXUSDCAD",
    "Bank of Canada Series"
)

dbutils.widgets.text(
    "schema_name",
    "default",
    "Schema"
)

dbutils.widgets.text(
    "bronze_table_name",
    "boc_bronze_raw",
    "Bronze Table"
)

dbutils.widgets.text(
    "silver_table_name",
    "boc_silver_exchange_rates",
    "Silver Table"
)

dbutils.widgets.text(
    "gold_table_name",
    "boc_gold_monthly_exchange_rates",
    "Gold Table"
)

dbutils.widgets.text(
    "audit_table_name",
    "boc_pipeline_audit",
    "Pipeline Audit Table"
)

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import functions as F


# ---------------------------------------------------------
# 1. Read parameters
# ---------------------------------------------------------

SERIES = dbutils.widgets.get("series")
schema_name = dbutils.widgets.get("schema_name")
bronze_table_name = dbutils.widgets.get("bronze_table_name")
silver_table_name = dbutils.widgets.get("silver_table_name")
gold_table_name = dbutils.widgets.get("gold_table_name")
audit_table_name = dbutils.widgets.get("audit_table_name")


# ---------------------------------------------------------
# 2. Build table names
# ---------------------------------------------------------

catalog_name = spark.catalog.currentCatalog()

bronze_table = (
    f"{catalog_name}."
    f"{schema_name}."
    f"{bronze_table_name}"
)

silver_table = (
    f"{catalog_name}."
    f"{schema_name}."
    f"{silver_table_name}"
)

gold_table = (
    f"{catalog_name}."
    f"{schema_name}."
    f"{gold_table_name}"
)

audit_table = (
    f"{catalog_name}."
    f"{schema_name}."
    f"{audit_table_name}"
)


# ---------------------------------------------------------
# 3. Make sure pipeline tables exist
# ---------------------------------------------------------

required_tables = {
    "Bronze": bronze_table,
    "Silver": silver_table,
    "Gold": gold_table
}

for layer_name, table_name in required_tables.items():

    if not spark.catalog.tableExists(table_name):

        raise ValueError(
            f"{layer_name} table does not exist: "
            f"{table_name}"
        )


# ---------------------------------------------------------
# 4. Read pipeline metrics
# ---------------------------------------------------------

bronze_df = spark.table(bronze_table)
silver_df = spark.table(silver_table)
gold_df = spark.table(gold_table)


bronze_file_count = (
    bronze_df
    .select("source_file")
    .distinct()
    .count()
)


silver_record_count = silver_df.count()


gold_row_count = gold_df.count()


latest_observation_date = (
    silver_df
    .select(
        F.max("observation_date").alias(
            "latest_observation_date"
        )
    )
    .first()["latest_observation_date"]
)


# ---------------------------------------------------------
# 5. Create one audit record
# ---------------------------------------------------------

audit_data = [
    (
        datetime.now(timezone.utc),
        SERIES,
        bronze_file_count,
        silver_record_count,
        gold_row_count,
        latest_observation_date,
        "SUCCESS"
    )
]


audit_schema = """
    run_logged_at timestamp,
    series string,
    bronze_file_count long,
    silver_record_count long,
    gold_row_count long,
    latest_observation_date date,
    status string
"""


audit_df = spark.createDataFrame(
    audit_data,
    schema=audit_schema
)


# ---------------------------------------------------------
# 6. Safely check existing audit schema
# ---------------------------------------------------------

if spark.catalog.tableExists(audit_table):

    existing_columns = set(
        spark.table(audit_table).columns
    )

    required_columns = set(
        audit_df.columns
    )

    missing_columns = (
        required_columns - existing_columns
    )

    if missing_columns:

        raise ValueError(
            f"Audit table is missing required columns: "
            f"{missing_columns}"
        )


# ---------------------------------------------------------
# 7. Append audit record
# ---------------------------------------------------------

(
    audit_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(audit_table)
)


print("Pipeline audit record created.")

print(
    f"Bronze files: {bronze_file_count}"
)

print(
    f"Silver records: {silver_record_count}"
)

print(
    f"Gold rows: {gold_row_count}"
)

print(
    f"Latest observation date: "
    f"{latest_observation_date}"
)

# COMMAND ----------

display(
    spark.table(
        f"{spark.catalog.currentCatalog()}.default.boc_pipeline_audit"
    )
)