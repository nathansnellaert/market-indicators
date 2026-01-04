"""Transform Shiller S&P 500 data."""

import pyarrow as pa
from subsets_utils import load_raw_file, upload_data, publish
from .test import test

DATASET_ID = "sp500_shiller"

METADATA = {
    "id": DATASET_ID,
    "title": "Shiller S&P 500 Historical Data",
    "description": "Robert Shiller's S&P 500 historical stock market data. Monthly data including prices, dividends, earnings, CPI, interest rates, and the cyclically adjusted price-to-earnings ratio (CAPE).",
    "column_descriptions": {
        "date": "Date (YYYY-MM-DD format)",
        "sp500": "S&P 500 index level",
        "dividend": "12-month trailing dividends",
        "earnings": "12-month trailing earnings",
        "cpi": "Consumer Price Index",
        "long_interest_rate": "10-year Treasury rate",
        "real_price": "Real (inflation-adjusted) S&P 500 price",
        "real_dividend": "Real (inflation-adjusted) dividends",
        "real_earnings": "Real (inflation-adjusted) earnings",
        "cape": "Cyclically Adjusted Price-to-Earnings ratio (Shiller P/E)",
    }
}


COLUMN_MAP = {
    "Date": "date",
    "SP500": "sp500",
    "Dividend": "dividend",
    "Earnings": "earnings",
    "Consumer Price Index": "cpi",
    "Long Interest Rate": "long_interest_rate",
    "Real Price": "real_price",
    "Real Dividend": "real_dividend",
    "Real Earnings": "real_earnings",
    "PE10": "cape",
}


def run():
    """Transform Shiller data to Arrow table."""
    csv_text = load_raw_file("shiller_data", extension="csv")

    lines = csv_text.strip().split('\n')
    header = lines[0].split(',')

    rows_by_date = {}
    for line in lines[1:]:
        values = line.split(',')
        if len(values) != len(header):
            continue

        row = {}
        for i, col in enumerate(header):
            val = values[i].strip()
            mapped_col = COLUMN_MAP.get(col, col.lower().replace(' ', '_'))
            if val == '' or val == 'NA':
                row[mapped_col] = None
            else:
                try:
                    row[mapped_col] = float(val)
                except ValueError:
                    row[mapped_col] = val
        # Keep last occurrence for each date (later revisions override earlier)
        rows_by_date[row['date']] = row

    if not rows_by_date:
        raise ValueError("No Shiller data found")

    # Sort by date to maintain chronological order
    rows = [rows_by_date[d] for d in sorted(rows_by_date.keys())]

    print(f"  Transformed {len(rows):,} rows")
    print(f"  Date range: {rows[0]['date']} to {rows[-1]['date']}")

    table = pa.Table.from_pylist(rows)

    test(table)

    upload_data(table, DATASET_ID, mode="overwrite")
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
