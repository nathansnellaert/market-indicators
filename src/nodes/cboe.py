"""CBOE volatility and strategy indices - ingest and transform.

Data source: https://www.cboe.com/
"""

import csv
from datetime import datetime
from io import StringIO
import pyarrow as pa
from subsets_utils import get, save_raw_file, load_raw_file, upload_data, validate
from subsets_utils.testing import assert_valid_date, assert_positive

BASE_URL = "https://cdn.cboe.com/api/global/us_indices/daily_prices"

# CBOE indices grouped by category
VOLATILITY_INDICES = ["VIX", "VIX1D", "VIX9D", "VIX3M", "VIX6M", "VIX1Y", "VVIX", "SKEW"]
COMMODITY_VOLATILITY = ["OVX", "GVZ"]
SINGLE_STOCK_VOLATILITY = ["VXAPL", "VXAZN", "VXEEM"]
BUYWRITE_INDICES = ["BXM", "BXMD", "BXMW", "BXY", "BXR", "BXN", "BXRC", "BXRD"]
PUTWRITE_INDICES = ["PUT", "PUTR", "WPUT", "WPTR", "PPUT"]
COLLAR_INDICES = ["CLL", "CLLZ", "CLLR"]
OTHER_STRATEGY = ["CMBO", "BFLY", "CNDR", "RXM", "LOVOL"]
VIX_STRATEGY = ["VPD", "VPN", "VSTG", "VXTH"]
SP500_STRATEGY = ["SPRO", "SPEN"]

ALL_INDICES = (
    VOLATILITY_INDICES + COMMODITY_VOLATILITY + SINGLE_STOCK_VOLATILITY +
    BUYWRITE_INDICES + PUTWRITE_INDICES + COLLAR_INDICES +
    OTHER_STRATEGY + VIX_STRATEGY + SP500_STRATEGY
    )

INDEX_CATEGORIES = {
    **{idx: "volatility" for idx in VOLATILITY_INDICES},
    **{idx: "commodity_volatility" for idx in COMMODITY_VOLATILITY},
    **{idx: "single_stock_volatility" for idx in SINGLE_STOCK_VOLATILITY},
    **{idx: "buywrite" for idx in BUYWRITE_INDICES},
    **{idx: "putwrite" for idx in PUTWRITE_INDICES},
    **{idx: "collar" for idx in COLLAR_INDICES},
    **{idx: "other_strategy" for idx in OTHER_STRATEGY},
    **{idx: "vix_strategy" for idx in VIX_STRATEGY},
    **{idx: "sp500_strategy" for idx in SP500_STRATEGY},
}

DATASET_ID = "cboe_volatility_indices"


def parse_date(date_str: str) -> str | None:
    """Parse date from MM/DD/YYYY format to YYYY-MM-DD."""
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str.strip(), "%m/%d/%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


def parse_float(value: str) -> float | None:
    """Parse float value, returning None for empty/invalid."""
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def test(table: pa.Table) -> None:
    """Validate CBOE indices output."""
    validate(table, {
        "columns": {
            "date": "string",
            "index": "string",
            "category": "string",
            "close": "double",
        },
        "not_null": ["date", "index", "category", "close"],
        "min_rows": 10000,
    })

    assert_valid_date(table, "date")
    assert_positive(table, "close")

    indices = set(table.column("index").to_pylist())
    assert len(indices) >= 10, f"Expected at least 10 indices, got {len(indices)}"

    dates = table.column("date").to_pylist()
    max_date = max(dates)
    assert max_date > "2024-01-01", f"Expected recent data, got latest: {max_date}"

    valid_categories = {
        "volatility", "commodity_volatility", "single_stock_volatility",
        "buywrite", "putwrite", "collar", "other_strategy",
        "vix_strategy", "sp500_strategy", "other"
    }
    categories = set(table.column("category").to_pylist())
    invalid = categories - valid_categories
    assert not invalid, f"Invalid categories: {invalid}"

    print(f"  Validated {len(table):,} CBOE records across {len(indices)} indices")


def process_index_file(index_name: str) -> list[dict]:
    """Process a single CBOE index CSV file."""
    try:
        csv_text = load_raw_file(index_name, extension="csv")
    except FileNotFoundError:
        return []

    records = []
    reader = csv.DictReader(StringIO(csv_text))

    for row in reader:
        date = parse_date(row.get("DATE", ""))
        if not date:
            continue

        close = parse_float(row.get("CLOSE") or row.get(index_name))
        if close is None:
            continue

        record = {
            "date": date,
            "index": index_name,
            "category": INDEX_CATEGORIES.get(index_name, "other"),
            "open": parse_float(row.get("OPEN")),
            "high": parse_float(row.get("HIGH")),
            "low": parse_float(row.get("LOW")),
            "close": close,
        }
        records.append(record)

    return records


def run():
    """Ingest and transform CBOE volatility indices."""
    # Ingest
    print(f"Fetching {len(ALL_INDICES)} CBOE indices...")
    for i, index in enumerate(ALL_INDICES, 1):
        print(f"  [{i}/{len(ALL_INDICES)}] Fetching {index}...")
        url = f"{BASE_URL}/{index}_History.csv"
        response = get(url)
        response.raise_for_status()
        save_raw_file(response.text, index, extension="csv")

    # Transform
    print("Transforming CBOE indices...")
    all_records = []

    for index_name in ALL_INDICES:
        records = process_index_file(index_name)
        if records:
            print(f"  {index_name}: {len(records):,} records")
            all_records.extend(records)

    if not all_records:
        raise ValueError("No CBOE index data found")

    all_records.sort(key=lambda r: (r["date"], r["index"]))

    table = pa.Table.from_pylist(all_records)
    print(f"  Total: {len(table):,} records across {len(set(r['index'] for r in all_records))} indices")

    test(table)
    upload_data(table, DATASET_ID, mode="overwrite")


NODES = {
    run: [],
}

if __name__ == "__main__":
    run()
