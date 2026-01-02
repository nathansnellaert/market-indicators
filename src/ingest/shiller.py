"""Ingest Shiller S&P 500 data."""

from subsets_utils import get, save_raw_file

DATA_URL = "https://datahub.io/core/s-and-p-500/r/data.csv"


def run():
    """Fetch Shiller S&P 500 CSV data."""
    print("  Fetching Shiller S&P 500 data...")
    response = get(DATA_URL)
    response.raise_for_status()
    save_raw_file(response.text, "shiller_data", extension="csv")
