"""Fetch The Economist's Big Mac Index data.

The Big Mac Index is an informal measure of purchasing power parity (PPP)
between currencies, using the price of a Big Mac as the benchmark.
"""

from subsets_utils import get, save_raw_file

BIG_MAC_URL = "https://raw.githubusercontent.com/TheEconomist/big-mac-data/master/output-data/big-mac-full-index.csv"


def run():
    """Fetch Big Mac Index CSV."""
    print("  Fetching Big Mac Index...")
    response = get(BIG_MAC_URL, timeout=60)
    response.raise_for_status()
    save_raw_file(response.text, "big_mac_index", extension="csv")
    print(f"    Saved big_mac_index.csv ({len(response.text):,} bytes)")
