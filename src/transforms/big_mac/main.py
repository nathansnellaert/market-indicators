"""Transform Big Mac Index data."""

import csv
from io import StringIO
import pyarrow as pa
from subsets_utils import load_raw_file, upload_data, publish
from .test import test

DATASET_ID = "big_mac_index"

METADATA = {
    "id": DATASET_ID,
    "title": "Big Mac Index (The Economist)",
    "description": "The Big Mac Index, published by The Economist since 1986, is an informal measure of purchasing power parity (PPP) between currencies. It uses the price of a McDonald's Big Mac as the benchmark for comparing currency valuations.",
    "column_descriptions": {
        "date": "Observation date (YYYY-MM-DD)",
        "country": "Country name",
        "iso_a3": "ISO 3166-1 alpha-3 country code",
        "currency_code": "ISO 4217 currency code",
        "local_price": "Big Mac price in local currency",
        "dollar_ex": "Exchange rate to USD",
        "dollar_price": "Big Mac price converted to USD",
        "usd_raw": "Raw PPP index vs USD (% over/under valuation)",
        "eur_raw": "Raw PPP index vs EUR (% over/under valuation)",
        "gbp_raw": "Raw PPP index vs GBP (% over/under valuation)",
        "jpy_raw": "Raw PPP index vs JPY (% over/under valuation)",
        "cny_raw": "Raw PPP index vs CNY (% over/under valuation)",
        "gdp_per_capita": "GDP per capita for GDP-adjusted calculations",
        "usd_adjusted": "GDP-adjusted PPP index vs USD",
    }
}


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


def run():
    """Transform Big Mac Index data."""
    csv_text = load_raw_file("big_mac_index", extension="csv")

    records = []
    reader = csv.DictReader(StringIO(csv_text))

    for row in reader:
        date = row.get("date", "").strip()
        if not date:
            continue

        # Skip if no price data
        dollar_price = parse_float(row.get("dollar_price"))
        if dollar_price is None:
            continue

        record = {
            "date": date,
            "country": row.get("name", "").strip(),
            "iso_a3": row.get("iso_a3", "").strip(),
            "currency_code": row.get("currency_code", "").strip(),
            "local_price": parse_float(row.get("local_price")),
            "dollar_ex": parse_float(row.get("dollar_ex")),
            "dollar_price": dollar_price,
            "usd_raw": parse_float(row.get("USD_raw")),
            "eur_raw": parse_float(row.get("EUR_raw")),
            "gbp_raw": parse_float(row.get("GBP_raw")),
            "jpy_raw": parse_float(row.get("JPY_raw")),
            "cny_raw": parse_float(row.get("CNY_raw")),
            "gdp_per_capita": parse_float(row.get("GDP_bigmac")),
            "usd_adjusted": parse_float(row.get("USD_adjusted")),
        }
        records.append(record)

    if not records:
        raise ValueError("No Big Mac Index data found")

    # Sort by date and country
    records.sort(key=lambda r: (r["date"], r["country"]))

    table = pa.Table.from_pylist(records)

    # Get unique countries and date range
    countries = set(r["country"] for r in records)
    dates = [r["date"] for r in records]

    print(f"  Transformed {len(records):,} records")
    print(f"  {len(countries)} countries, {min(dates)} to {max(dates)}")

    test(table)
    upload_data(table, DATASET_ID, mode="overwrite")
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
