# Databricks notebook source
import json
from datetime import datetime, timedelta, timezone

import requests

# ---------------------------------------------------------
# 1. Notebook parameters
# ---------------------------------------------------------

dbutils.widgets.text(
    "series",
    "FXUSDCAD",
    "Bank of Canada Series"
)

dbutils.widgets.text(
    "default_start_date",
    "2024-01-01",
    "Default Start Date"
)

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


SERIES = dbutils.widgets.get("series")
DEFAULT_START_DATE = dbutils.widgets.get("default_start_date")
schema_name = dbutils.widgets.get("schema_name")
volume_name = dbutils.widgets.get("volume_name")


# ---------------------------------------------------------
# 2. Fixed configuration
# ---------------------------------------------------------

BASE_URL = "https://www.bankofcanada.ca/valet"

catalog_name = spark.catalog.currentCatalog()

silver_table = (
    f"{catalog_name}."
    f"{schema_name}."
    f"boc_silver_exchange_rates"
)

volume_path = (
    f"/Volumes/{catalog_name}/"
    f"{schema_name}/"
    f"{volume_name}"
)


# ---------------------------------------------------------
# 3. Determine incremental start date
# ---------------------------------------------------------

if spark.catalog.tableExists(silver_table):

    latest_date = (
        spark.table(silver_table)
        .selectExpr(
            "MAX(observation_date) AS latest_date"
        )
        .first()["latest_date"]
    )

    if latest_date:
        START_DATE = (
            latest_date + timedelta(days=1)
        ).isoformat()
    else:
        START_DATE = DEFAULT_START_DATE

else:
    START_DATE = DEFAULT_START_DATE


print("Series:", SERIES)
print("API start date:", START_DATE)
print("Raw volume:", volume_path)


# ---------------------------------------------------------
# 4. Call Bank of Canada API
# ---------------------------------------------------------

api_url = (
    f"{BASE_URL}/observations/{SERIES}/json"
    f"?start_date={START_DATE}"
)

response = requests.get(
    api_url,
    timeout=30
)

response.raise_for_status()

payload = response.json()

observations = payload.get(
    "observations",
    []
)

# ---------------------------------------------------------
# 5. Save only when new observations exist
# ---------------------------------------------------------

if not observations:

    print("No new observations available.")

else:

    retrieved_at = datetime.now(
        timezone.utc
    )

    raw_data = {
        "metadata": {
            "source": "Bank of Canada Valet API",
            "series": SERIES,
            "start_date": START_DATE,
            "retrieved_at_utc": retrieved_at.isoformat()
        },
        "payload": payload
    }

    timestamp = retrieved_at.strftime(
        "%Y%m%dT%H%M%SZ"
    )

    file_name = (
        f"{SERIES}_{timestamp}.json"
    )

    file_path = (
        f"{volume_path}/{file_name}"
    )

    with open(
        file_path,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            raw_data,
            file,
            indent=2
        )

    print(
        f"Received {len(observations)} new observations."
    )

    print(
        f"Raw data saved to: {file_path}"
    )