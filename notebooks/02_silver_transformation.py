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

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ---------------------------------------------------------
# 1. Read parameters
# ---------------------------------------------------------

SERIES = dbutils.widgets.get("series")
schema_name = dbutils.widgets.get("schema_name")
bronze_table_name = dbutils.widgets.get("bronze_table_name")
silver_table_name = dbutils.widgets.get("silver_table_name")


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

processed_files_table = (
    f"{catalog_name}."
    f"{schema_name}."
    f"boc_silver_processed_files"
)


# ---------------------------------------------------------
# 3. Read Bronze
# ---------------------------------------------------------

bronze_df = spark.table(bronze_table)


# ---------------------------------------------------------
# 4. Verify Bronze has the columns Silver needs
# ---------------------------------------------------------

required_bronze_columns = {
    "source_file",
    "bronze_loaded_at"
}

bronze_columns = set(bronze_df.columns)

missing_bronze_columns = (
    required_bronze_columns - bronze_columns
)

if missing_bronze_columns:
    raise ValueError(
        f"Bronze is missing required columns: "
        f"{missing_bronze_columns}"
    )


# ---------------------------------------------------------
# 5. Function: Bronze -> Silver transformation
# ---------------------------------------------------------

def transform_to_silver(df):

    exploded_df = (
        df.select(
            F.explode(
                "payload.observations"
            ).alias("observation"),

            "source_file",
            "bronze_loaded_at"
        )
    )

    transformed_df = (
        exploded_df.select(

            F.to_date(
                F.col("observation.d")
            ).alias("observation_date"),

            F.lit(
                SERIES
            ).alias("series"),

            F.col(
                f"observation.{SERIES}.v"
            ).cast("double").alias("value"),

            F.col(
                "source_file"
            ),

            F.col(
                "bronze_loaded_at"
            ),

            F.current_timestamp().alias(
                "silver_loaded_at"
            )
        )
    )

    return transformed_df


# ---------------------------------------------------------
# 6. Check existing Silver schema
# ---------------------------------------------------------

required_silver_columns = {
    "observation_date",
    "series",
    "value",
    "source_file",
    "silver_loaded_at"
}


silver_exists = spark.catalog.tableExists(
    silver_table
)

processed_files_table_exists = (
    spark.catalog.tableExists(
        processed_files_table
    )
)


if silver_exists:

    existing_silver_df = spark.table(
        silver_table
    )

    existing_silver_columns = set(
        existing_silver_df.columns
    )

    missing_silver_columns = (
        required_silver_columns
        - existing_silver_columns
    )

else:

    missing_silver_columns = (
        required_silver_columns
    )


print(
    "Missing Silver columns:",
    missing_silver_columns
)


# ---------------------------------------------------------
# 7. Decide whether one-time initialization/rebuild
#    is needed
# ---------------------------------------------------------

needs_rebuild = (
    not silver_exists
    or bool(missing_silver_columns)
    or not processed_files_table_exists
)


# ---------------------------------------------------------
# 8. ONE-TIME INITIALIZATION / REBUILD
# ---------------------------------------------------------

if needs_rebuild:

    print(
        "Silver requires initialization/rebuild."
    )


    # Transform all existing Bronze data
    rebuilt_silver_df = (
        transform_to_silver(
            bronze_df
        )
    )


    # -----------------------------------------------------
    # Keep one row per business key
    #
    # Business key:
    # observation_date + series
    #
    # If multiple raw files contain the same date,
    # prefer the newest Bronze record.
    # -----------------------------------------------------

    window_spec = (
        Window
        .partitionBy(
            "observation_date",
            "series"
        )
        .orderBy(
            F.col(
                "bronze_loaded_at"
            ).desc(),

            F.col(
                "source_file"
            ).desc()
        )
    )


    rebuilt_silver_df = (
        rebuilt_silver_df
        .withColumn(
            "row_number",
            F.row_number().over(
                window_spec
            )
        )
        .filter(
            F.col("row_number") == 1
        )
        .drop(
            "row_number",
            "bronze_loaded_at"
        )
    )


    # -----------------------------------------------------
    # Rebuild Silver with the new schema
    # -----------------------------------------------------

    (
        rebuilt_silver_df.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(
            silver_table
        )
    )


    # -----------------------------------------------------
    # Create processed-files tracker
    #
    # This remembers ALL Bronze files that Silver
    # processed during the rebuild.
    # -----------------------------------------------------

    processed_files_df = (
        bronze_df
        .select(
            "source_file"
        )
        .distinct()
        .withColumn(
            "silver_processed_at",
            F.current_timestamp()
        )
    )


    (
        processed_files_df.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(
            processed_files_table
        )
    )


    print(
        "Silver table rebuilt with incremental tracking."
    )

    print(
        "Processed-files tracker created."
    )


# ---------------------------------------------------------
# 9. NORMAL INCREMENTAL RUN
# ---------------------------------------------------------

else:

    # -----------------------------------------------------
    # Get files already processed by Silver
    # -----------------------------------------------------

    processed_files_df = (
        spark.table(
            processed_files_table
        )
        .select(
            "source_file"
        )
        .distinct()
    )


    # -----------------------------------------------------
    # Find Bronze files that Silver has NOT processed
    # -----------------------------------------------------

    new_bronze_df = (
        bronze_df
        .join(
            processed_files_df,
            on="source_file",
            how="left_anti"
        )
    )


    new_file_count = (
        new_bronze_df
        .select(
            "source_file"
        )
        .distinct()
        .count()
    )


    print(
        "New Bronze files for Silver:",
        new_file_count
    )


    # -----------------------------------------------------
    # 10. Nothing new -> stop normally
    # -----------------------------------------------------

    if new_file_count == 0:

        print(
            "No new Bronze files for Silver."
        )


    # -----------------------------------------------------
    # 11. Transform new Bronze files
    # -----------------------------------------------------

    else:

        incoming_silver_df = (
            transform_to_silver(
                new_bronze_df
            )
        )


        # -------------------------------------------------
        # Make sure MERGE source contains only one row
        # per business key.
        #
        # Important because Delta MERGE should not receive
        # multiple source records for the same key.
        # -------------------------------------------------

        window_spec = (
            Window
            .partitionBy(
                "observation_date",
                "series"
            )
            .orderBy(
                F.col(
                    "bronze_loaded_at"
                ).desc(),

                F.col(
                    "source_file"
                ).desc()
            )
        )


        incoming_silver_df = (
            incoming_silver_df
            .withColumn(
                "row_number",
                F.row_number().over(
                    window_spec
                )
            )
            .filter(
                F.col("row_number") == 1
            )
            .drop(
                "row_number",
                "bronze_loaded_at"
            )
        )


        # -------------------------------------------------
        # 12. Create temporary view for MERGE
        # -------------------------------------------------

        incoming_silver_df.createOrReplaceTempView(
            "incoming_silver"
        )


        # -------------------------------------------------
        # 13. Delta MERGE
        #
        # Business key:
        # observation_date + series
        # -------------------------------------------------

        spark.sql(
            f"""
            MERGE INTO {silver_table} AS target

            USING incoming_silver AS source

            ON target.observation_date =
               source.observation_date

            AND target.series =
                source.series


            WHEN MATCHED THEN
                UPDATE SET

                    target.value =
                        source.value,

                    target.source_file =
                        source.source_file,

                    target.silver_loaded_at =
                        source.silver_loaded_at


            WHEN NOT MATCHED THEN
                INSERT (
                    observation_date,
                    series,
                    value,
                    source_file,
                    silver_loaded_at
                )

                VALUES (
                    source.observation_date,
                    source.series,
                    source.value,
                    source.source_file,
                    source.silver_loaded_at
                )
            """
        )


        # -------------------------------------------------
        # 14. Mark those Bronze files as processed
        #
        # Do this AFTER successful MERGE.
        # -------------------------------------------------

        newly_processed_files_df = (
            new_bronze_df
            .select(
                "source_file"
            )
            .distinct()
            .withColumn(
                "silver_processed_at",
                F.current_timestamp()
            )
        )


        (
            newly_processed_files_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(
                processed_files_table
            )
        )


        print(
            "Silver incremental MERGE completed."
        )

        print(
            f"{new_file_count} Bronze file(s) "
            f"marked as processed."
        )