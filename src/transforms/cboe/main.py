"""Transform CBOE volatility indices data."""

import csv
from datetime import datetime
from io import StringIO
import pyarrow as pa
from subsets_utils import load_raw_file, list_raw_files, upload_data, publish
from .test import test

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

METADATA = {
    "id": DATASET_ID,
    "title": "CBOE Volatility and Strategy Indices (Daily)",
    "description": "Daily values for CBOE volatility indices (VIX, VVIX, SKEW) and strategy benchmark indices (BuyWrite, PutWrite, Collar). Includes open, high, low, close values where available.",
    "column_descriptions": {
        "date": "Trading date (YYYY-MM-DD)",
        "index": "CBOE index symbol (e.g., VIX, SKEW, BXM)",
        "category": "Index category (volatility, buywrite, putwrite, collar, etc.)",
        "open": "Opening value",
        "high": "Intraday high",
        "low": "Intraday low",
        "close": "Closing value",
    }
}


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

        # Get close value - required
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
    """Transform all CBOE index data to unified dataset."""
    all_records = []

    for index_name in ALL_INDICES:
        records = process_index_file(index_name)
        if records:
            print(f"  {index_name}: {len(records):,} records")
            all_records.extend(records)

    if not all_records:
        raise ValueError("No CBOE index data found")

    # Sort by date and index
    all_records.sort(key=lambda r: (r["date"], r["index"]))

    table = pa.Table.from_pylist(all_records)
    print(f"  Total: {len(table):,} records across {len(set(r['index'] for r in all_records))} indices")

    test(table)
    upload_data(table, DATASET_ID, mode="overwrite")
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
