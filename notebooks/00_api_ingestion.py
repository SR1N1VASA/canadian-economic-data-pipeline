# Databricks notebook source
import json
from datetime import datetime, timedelta, timezone

import requests


SERIES = "FXUSDCAD"
DEFAULT_START_DATE = "2024-01-01"
BASE_URL = "https://www.bankofcanada.ca/valet"

catalog_name = spark.catalog.currentCatalog()

silver_table = (
    f"`{catalog_name}`."
    f"`default`."
    f"`boc_silver_exchange_rates`"
)

volume_path = (
    f"/Volumes/{catalog_name}/default/boc_raw"
)


# Determine incremental start date
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


print("Fetching observations from:", START_DATE)


# Call Bank of Canada API
api_url = (
    f"{BASE_URL}/observations/{SERIES}/json"
    f"?start_date={START_DATE}"
)

response = requests.get(api_url, timeout=30)
response.raise_for_status()

payload = response.json()

observations = payload.get("observations", [])


# Only save a raw file if new data exists
if not observations:

    print("No new observations available.")

else:

    retrieved_at = datetime.now(timezone.utc)

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
        f"Saved {len(observations)} observations "
        f"to {file_path}"
    )