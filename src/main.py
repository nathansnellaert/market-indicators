"""Market Indicators Data Connector

Aggregates market and economic indicator data from multiple sources:

- Shiller: Long-term stock market data, CAPE ratio, interest rates
- CBOE: VIX and volatility indices
- Baker Hughes: Oil & gas rig counts
- UMich: Consumer sentiment index
- Big Mac Index: Purchasing power parity indicator
"""

import argparse
import os

os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

from subsets_utils import validate_environment
from ingest import shiller as ingest_shiller
from ingest import cboe as ingest_cboe
from ingest import rig_data as ingest_rigs
from ingest import sentiment as ingest_sentiment
from ingest import big_mac_index as ingest_big_mac
from transforms import shiller as transform_shiller
from transforms import rig_counts as transform_rigs
from transforms import sentiment as transform_sentiment
from transforms import cboe as transform_cboe
from transforms import big_mac as transform_big_mac


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest-only", action="store_true", help="Only fetch data from API")
    parser.add_argument("--transform-only", action="store_true", help="Only transform existing raw data")
    args = parser.parse_args()

    validate_environment()

    should_ingest = not args.transform_only
    should_transform = not args.ingest_only

    if should_ingest:
        print("\n=== Phase 1: Ingest ===")

        print("\n--- Shiller Stock Market Data ---")
        ingest_shiller.run()

        print("\n--- CBOE Volatility Indices ---")
        ingest_cboe.run()

        print("\n--- Baker Hughes Rig Counts ---")
        ingest_rigs.run()

        print("\n--- UMich Consumer Sentiment ---")
        ingest_sentiment.run()

        print("\n--- Big Mac Index ---")
        ingest_big_mac.run()

    if should_transform:
        print("\n=== Phase 2: Transform ===")

        print("\n--- Shiller Data ---")
        transform_shiller.run()

        print("\n--- CBOE Volatility Indices ---")
        transform_cboe.run()

        print("\n--- Rig Counts ---")
        transform_rigs.run()

        print("\n--- Consumer Sentiment ---")
        transform_sentiment.run()

        print("\n--- Big Mac Index ---")
        transform_big_mac.run()


if __name__ == "__main__":
    main()
