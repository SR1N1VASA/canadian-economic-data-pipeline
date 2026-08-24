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
    "volume_name",
    "boc_raw",
    "Raw Volume"
)

dbutils.widgets.text(
    "bronze_table_name",
    "boc_bronze_raw",
    "Bronze Table"
)

# COMMAND ----------

from pyspark.sql import functions as F


# ---------------------------------------------------------
# 1. Read parameters
# ---------------------------------------------------------

schema_name = dbutils.widgets.get("schema_name")
volume_name = dbutils.widgets.get("volume_name")
bronze_table_name = dbutils.widgets.get("bronze_table_name")


# ---------------------------------------------------------
# 2. Build paths/table names
# ---------------------------------------------------------

catalog_name = spark.catalog.currentCatalog()

raw_path = (
    f"/Volumes/{catalog_name}/"
    f"{schema_name}/"
    f"{volume_name}/"
)

bronze_table = (
    f"{catalog_name}."
    f"{schema_name}."
    f"{bronze_table_name}"
)


# ---------------------------------------------------------
# 3. Read raw JSON files and capture source file
# ---------------------------------------------------------

raw_df = (
    spark.read
    .option("multiline", "true")
    .json(raw_path)
    .select(
        "*",
        F.col("_metadata.file_path").alias("source_file")
    )
)


# ---------------------------------------------------------
# 4. Add Bronze processing metadata
# ---------------------------------------------------------

incoming_bronze_df = (
    raw_df
    .withColumn(
        "bronze_loaded_at",
        F.current_timestamp()
    )
)

# ---------------------------------------------------------
# 5. Check whether Bronze already exists
# ---------------------------------------------------------

if not spark.catalog.tableExists(bronze_table):

    # First-ever Bronze load
    (
        incoming_bronze_df.write
        .format("delta")
        .saveAsTable(bronze_table)
    )

    print("Bronze table created.")
    print("All current raw files were loaded.")


else:

    # -----------------------------------------------------
    # 6. Check whether old Bronze has source_file
    # -----------------------------------------------------

    existing_bronze_df = spark.table(bronze_table)

    if "source_file" not in existing_bronze_df.columns:

        # Our existing Bronze table was created before
        # we started tracking source filenames.
        #
        # Rebuild it once from the raw Volume.

        (
            incoming_bronze_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(bronze_table)
        )

        print(
            "Bronze table rebuilt with source_file tracking."
        )


    else:

        # -------------------------------------------------
        # 7. Get files Bronze already processed
        # -------------------------------------------------

        processed_files_df = (
            existing_bronze_df
            .select("source_file")
            .distinct()
        )


        # -------------------------------------------------
        # 8. Keep only files not already in Bronze
        # -------------------------------------------------

        new_bronze_df = (
            incoming_bronze_df
            .join(
                processed_files_df,
                on="source_file",
                how="left_anti"
            )
        )


        # -------------------------------------------------
        # 9. Count distinct new files
        # -------------------------------------------------

        new_file_count = (
            new_bronze_df
            .select("source_file")
            .distinct()
            .count()
        )

        print(
            "New raw files:",
            new_file_count
        )


        # -------------------------------------------------
        # 10. Append only new files
        # -------------------------------------------------

        if new_file_count > 0:

            (
                new_bronze_df.write
                .format("delta")
                .mode("append")
                .saveAsTable(bronze_table)
            )

            print(
                f"Processed {new_file_count} new raw file(s)."
            )

        else:

            print("No new raw files to process.")