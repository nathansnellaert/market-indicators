
"""Ingest UMich Consumer Sentiment data."""

from subsets_utils import get, save_raw_json

BASE_URL = "https://www.sca.isr.umich.edu/files"

FILES = [
    ("tbmics.csv", "consumer_sentiment"),
    ("tbmiccice.csv", "sentiment_components"),
    ("tbmpx1px5.csv", "inflation_expectations"),
]


def run():
    """Fetch all UMich consumer sentiment CSV files."""
    all_data = {}

    for filename, key in FILES:
        print(f"  Fetching {filename}...")
        url = f"{BASE_URL}/{filename}"
        response = get(url)
        response.raise_for_status()
        all_data[key] = response.text

    save_raw_json(all_data, "sentiment_data")
