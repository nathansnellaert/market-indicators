"""Transform UMich Consumer Sentiment data."""

import csv
from io import StringIO
import pyarrow as pa
from subsets_utils import load_raw_json, upload_data, publish
from .test import test_consumer_sentiment, test_sentiment_components, test_inflation_expectations

START_YEAR = 1978

MONTH_MAP = {
    "January": 1, "February": 2, "March": 3, "April": 4,
    "May": 5, "June": 6, "July": 7, "August": 8,
    "September": 9, "October": 10, "November": 11, "December": 12
}

# Dataset configurations
CONSUMER_SENTIMENT = {
    "id": "umich_consumer_sentiment",
    "title": "University of Michigan Consumer Sentiment Index (Monthly)",
    "description": "Monthly Index of Consumer Sentiment (ICS) from the University of Michigan Survey of Consumers. Measures consumer confidence about personal finances and business conditions.",
    "column_descriptions": {
        "month": "Month of survey (YYYY-MM)",
        "index": "Index of Consumer Sentiment (1966=100)",
    }
}

SENTIMENT_COMPONENTS = {
    "id": "umich_sentiment_components",
    "title": "University of Michigan Sentiment Components (Monthly)",
    "description": "Component indices from the University of Michigan Survey of Consumers. ICC measures current conditions, ICE measures consumer expectations.",
    "column_descriptions": {
        "month": "Month of survey (YYYY-MM)",
        "index_current_conditions": "Index of Current Economic Conditions",
        "index_expectations": "Index of Consumer Expectations",
    }
}

INFLATION_EXPECTATIONS = {
    "id": "umich_inflation_expectations",
    "title": "University of Michigan Inflation Expectations (Monthly)",
    "description": "Consumer inflation expectations from the University of Michigan Survey of Consumers. Median expected price changes over 1-year and 5-year horizons.",
    "column_descriptions": {
        "month": "Month of survey (YYYY-MM)",
        "inflation_1yr": "Expected inflation over next 12 months (percent)",
        "inflation_5yr": "Expected inflation over next 5 years (percent)",
    }
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


def run():
    """Transform all sentiment data files."""
    raw_data = load_raw_json("sentiment_data")

    # Consumer Sentiment Index
    process_consumer_sentiment(raw_data["consumer_sentiment"])

    # Sentiment Components
    process_sentiment_components(raw_data["sentiment_components"])

    # Inflation Expectations
    process_inflation_expectations(raw_data["inflation_expectations"])


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

    upload_data(table, CONSUMER_SENTIMENT["id"], mode="overwrite")
    publish(CONSUMER_SENTIMENT["id"], CONSUMER_SENTIMENT)


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

    upload_data(table, SENTIMENT_COMPONENTS["id"], mode="overwrite")
    publish(SENTIMENT_COMPONENTS["id"], SENTIMENT_COMPONENTS)


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

    upload_data(table, INFLATION_EXPECTATIONS["id"], mode="overwrite")
    publish(INFLATION_EXPECTATIONS["id"], INFLATION_EXPECTATIONS)


if __name__ == "__main__":
    run()
