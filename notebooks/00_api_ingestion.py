# Databricks notebook source
import json
from datetime import datetime, timezone

import requests

SERIES = "FXUSDCAD"
START_DATE = "2024-01-01"
BASE_URL = "https://www.bankofcanada.ca/valet"

api_url = (
    f"{BASE_URL}/observations/{SERIES}/json"
    f"?start_date={START_DATE}"
)

response = requests.get(api_url, timeout=30)
response.raise_for_status()

payload = response.json()

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

catalog_name = spark.catalog.currentCatalog()

volume_path = (
    f"/Volumes/{catalog_name}/default/boc_raw"
)

timestamp = retrieved_at.strftime("%Y%m%dT%H%M%SZ")
file_name = f"{SERIES}_{timestamp}.json"
file_path = f"{volume_path}/{file_name}"

with open(file_path, "w", encoding="utf-8") as file:
    json.dump(raw_data, file, indent=2)

print(f"Saved raw data to: {file_path}")