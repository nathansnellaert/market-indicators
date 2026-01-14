"""Shiller S&P 500 historical data - ingest and transform.

Data source: https://datahub.io/core/s-and-p-500
"""

import pyarrow as pa
from subsets_utils import get, save_raw_file, load_raw_file, upload_data, validate

DATA_URL = "https://datahub.io/core/s-and-p-500/r/data.csv"
DATASET_ID = "sp500_shiller"

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


def test(table: pa.Table) -> None:
    """Validate Shiller S&P 500 output."""
    validate(table, {
        "columns": {
            "date": "string",
            "sp500": "double",
            "cpi": "double",
            "cape": "double",
        },
        "not_null": ["date"],
        "unique": ["date"],
        "min_rows": 1000,
    })

    dates = table.column("date").to_pylist()
    assert dates[0] < dates[-1], "Data should be chronologically sorted"

    cape_values = [c for c in table.column("cape").to_pylist() if c]
    assert min(cape_values) > 0, "CAPE should be positive"
    assert max(cape_values) < 100, "CAPE should be reasonable"

    print(f"  Validated {len(table):,} Shiller records")


def run():
    """Ingest and transform Shiller S&P 500 data."""
    # Ingest
    print("Fetching Shiller S&P 500 data...")
    response = get(DATA_URL)
    response.raise_for_status()
    save_raw_file(response.text, "shiller_data", extension="csv")

    # Transform
    print("Transforming Shiller data...")
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
        rows_by_date[row['date']] = row

    if not rows_by_date:
        raise ValueError("No Shiller data found")

    rows = [rows_by_date[d] for d in sorted(rows_by_date.keys())]

    print(f"  Transformed {len(rows):,} rows")
    print(f"  Date range: {rows[0]['date']} to {rows[-1]['date']}")

    table = pa.Table.from_pylist(rows)
    test(table)
    upload_data(table, DATASET_ID, mode="overwrite")


NODES = {
    run: [],
}

if __name__ == "__main__":
    run()
