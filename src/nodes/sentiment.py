"""UMich Consumer Sentiment data - ingest and transform.

Data source: https://www.sca.isr.umich.edu/
"""

import csv
from io import StringIO
import pyarrow as pa
from subsets_utils import get, save_raw_json, load_raw_json, upload_data, validate
from subsets_utils.testing import assert_valid_month

BASE_URL = "https://www.sca.isr.umich.edu/files"
START_YEAR = 1978

FILES = [
    ("tbmics.csv", "consumer_sentiment"),
    ("tbmiccice.csv", "sentiment_components"),
    ("tbmpx1px5.csv", "inflation_expectations"),
]

MONTH_MAP = {
    "January": 1, "February": 2, "March": 3, "April": 4,
    "May": 5, "June": 6, "July": 7, "August": 8,
    "September": 9, "October": 10, "November": 11, "December": 12
}


def parse_date(month_str, year_str):
    """Convert month name and year to YYYY-MM."""
    month = MONTH_MAP.get(month_str.strip())
    year = int(year_str.strip())
    if month:
        return f"{year}-{month:02d}"
    return None


def parse_float(value):
    """Parse a float value, returning None for empty/invalid."""
    if value is None:
        return None
    value = value.strip()
    if not value or value == ".":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def test_consumer_sentiment(table: pa.Table) -> None:
    """Validate consumer sentiment output."""
    validate(table, {
        "columns": {
            "month": "string",
            "index": "double",
        },
        "not_null": ["month", "index"],
        "min_rows": 100,
    })
    assert_valid_month(table, "month")
    print(f"  Validated {len(table):,} consumer sentiment records")


def test_sentiment_components(table: pa.Table) -> None:
    """Validate sentiment components output."""
    validate(table, {
        "columns": {
            "month": "string",
            "index_current_conditions": "double",
            "index_expectations": "double",
        },
        "not_null": ["month"],
        "min_rows": 100,
    })
    assert_valid_month(table, "month")
    print(f"  Validated {len(table):,} sentiment component records")


def test_inflation_expectations(table: pa.Table) -> None:
    """Validate inflation expectations output."""
    validate(table, {
        "columns": {
            "month": "string",
            "inflation_1yr": "double",
            "inflation_5yr": "double",
        },
        "not_null": ["month"],
        "min_rows": 100,
    })
    assert_valid_month(table, "month")
    print(f"  Validated {len(table):,} inflation expectation records")


def process_consumer_sentiment(csv_text):
    """Transform consumer sentiment data."""
    reader = csv.DictReader(StringIO(csv_text))

    processed = []
    for row in reader:
        year = int(row.get("YYYY", "0").strip())
        if year < START_YEAR:
            continue

        month = parse_date(row.get("Month", ""), row.get("YYYY", ""))
        if not month:
            continue

        value = parse_float(row.get("ICS_ALL"))
        if value is None:
            continue

        processed.append({
            "month": month,
            "index": value,
        })

    if not processed:
        raise ValueError("No consumer sentiment data found")

    print(f"  Transformed {len(processed):,} consumer sentiment observations")
    table = pa.Table.from_pylist(processed)

    test_consumer_sentiment(table)
    upload_data(table, "umich_consumer_sentiment", mode="overwrite")


def process_sentiment_components(csv_text):
    """Transform sentiment components data."""
    reader = csv.DictReader(StringIO(csv_text))

    processed = []
    for row in reader:
        year = int(row.get("YYYY", "0").strip())
        if year < START_YEAR:
            continue

        month = parse_date(row.get("Month", ""), row.get("YYYY", ""))
        if not month:
            continue

        icc = parse_float(row.get("ICC"))
        ice = parse_float(row.get("ICE"))

        if icc is None and ice is None:
            continue

        processed.append({
            "month": month,
            "index_current_conditions": icc,
            "index_expectations": ice,
        })

    if not processed:
        raise ValueError("No sentiment components data found")

    print(f"  Transformed {len(processed):,} sentiment component observations")
    table = pa.Table.from_pylist(processed)

    test_sentiment_components(table)
    upload_data(table, "umich_sentiment_components", mode="overwrite")


def process_inflation_expectations(csv_text):
    """Transform inflation expectations data."""
    reader = csv.DictReader(StringIO(csv_text))

    processed = []
    for row in reader:
        year = int(row.get("YYYY", "0").strip())
        if year < START_YEAR:
            continue

        month = parse_date(row.get("Month", ""), row.get("YYYY", ""))
        if not month:
            continue

        px1 = parse_float(row.get("PX_MD"))
        px5 = parse_float(row.get("PX5_MD"))

        if px1 is None and px5 is None:
            continue

        processed.append({
            "month": month,
            "inflation_1yr": px1,
            "inflation_5yr": px5,
        })

    if not processed:
        raise ValueError("No inflation expectations data found")

    print(f"  Transformed {len(processed):,} inflation expectation observations")
    table = pa.Table.from_pylist(processed)

    test_inflation_expectations(table)
    upload_data(table, "umich_inflation_expectations", mode="overwrite")


def run():
    """Ingest and transform UMich Consumer Sentiment data."""
    # Ingest
    print("Fetching UMich Consumer Sentiment data...")
    all_data = {}

    for filename, key in FILES:
        print(f"  Fetching {filename}...")
        url = f"{BASE_URL}/{filename}"
        response = get(url)
        response.raise_for_status()
        all_data[key] = response.text

    save_raw_json(all_data, "sentiment_data")

    # Transform
    print("Transforming sentiment data...")
    raw_data = load_raw_json("sentiment_data")

    process_consumer_sentiment(raw_data["consumer_sentiment"])
    process_sentiment_components(raw_data["sentiment_components"])
    process_inflation_expectations(raw_data["inflation_expectations"])


NODES = {
    run: [],
}

if __name__ == "__main__":
    run()
