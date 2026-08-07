"""Retrieve public economic data from the Bank of Canada Valet API.

This is the initial ingestion component for the Canadian Economic Data
Pipeline. It downloads a series and stores the raw API response together
with ingestion metadata.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


BASE_URL = "https://www.bankofcanada.ca/valet"
DEFAULT_SERIES = "FXUSDCAD"
DEFAULT_OUTPUT_DIRECTORY = Path("data/raw")


def fetch_observations(
    series: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    """Retrieve observations for one Bank of Canada series."""

    url = f"{BASE_URL}/observations/{series}/json"

    params: dict[str, str] = {}

    if start_date:
        params["start_date"] = start_date

    if end_date:
        params["end_date"] = end_date

    response = requests.get(
        url,
        params=params,
        timeout=30,
        headers={
            "User-Agent": "canadian-economic-data-pipeline/0.1"
        },
    )

    response.raise_for_status()
    return response.json()


def save_raw_response(
    payload: dict[str, Any],
    series: str,
    start_date: str | None,
    end_date: str | None,
    output_directory: Path,
) -> Path:
    """Save the source response with ingestion metadata."""

    retrieved_at = datetime.now(timezone.utc)
    timestamp = retrieved_at.strftime("%Y%m%dT%H%M%SZ")

    output_directory.mkdir(parents=True, exist_ok=True)
    output_path = output_directory / f"{series}_{timestamp}.json"

    raw_record = {
        "metadata": {
            "source": "Bank of Canada Valet API",
            "series": series,
            "start_date": start_date,
            "end_date": end_date,
            "retrieved_at_utc": retrieved_at.isoformat(),
        },
        "payload": payload,
    }

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(raw_record, file, indent=2, ensure_ascii=False)

    return output_path


def parse_arguments() -> argparse.Namespace:
    """Read command-line arguments."""

    parser = argparse.ArgumentParser(
        description="Retrieve observations from the Bank of Canada Valet API."
    )

    parser.add_argument(
        "--series",
        default=DEFAULT_SERIES,
        help=f"Bank of Canada series code. Default: {DEFAULT_SERIES}",
    )

    parser.add_argument(
        "--start-date",
        default="2024-01-01",
        help="Optional starting date in YYYY-MM-DD format.",
    )

    parser.add_argument(
        "--end-date",
        default=None,
        help="Optional ending date in YYYY-MM-DD format.",
    )

    parser.add_argument(
        "--output-directory",
        type=Path,
        default=DEFAULT_OUTPUT_DIRECTORY,
        help="Directory where the raw JSON response will be saved.",
    )

    return parser.parse_args()


def main() -> int:
    """Run the ingestion process."""

    arguments = parse_arguments()

    try:
        payload = fetch_observations(
            series=arguments.series,
            start_date=arguments.start_date,
            end_date=arguments.end_date,
        )

        output_path = save_raw_response(
            payload=payload,
            series=arguments.series,
            start_date=arguments.start_date,
            end_date=arguments.end_date,
            output_directory=arguments.output_directory,
        )

        observation_count = len(payload.get("observations", []))

        print(f"Retrieved {observation_count} observations.")
        print(f"Raw response saved to: {output_path}")

        return 0

    except requests.Timeout:
        print(
            "The request to the Bank of Canada API timed out.",
            file=sys.stderr,
        )
        return 1

    except requests.HTTPError as error:
        print(
            f"The API returned an HTTP error: {error}",
            file=sys.stderr,
        )
        return 1

    except requests.RequestException as error:
        print(
            f"Unable to retrieve Bank of Canada data: {error}",
            file=sys.stderr,
        )
        return 1

    except (OSError, ValueError) as error:
        print(
            f"Unable to save or process the response: {error}",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())